"""Silver: typed, validated and de-duplicated entities.

- customers       SCD type 2 (history of tier / credit-term changes)
- shipments       SCD type 1 via AUTO CDC (latest booking version wins)
- events/customs/invoices  append-only streams with expectations
Rows failing hard rules are dropped; failing shipments are kept in a quarantine table.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

EVENT_CODES = "('BKD','PUP','DEP','EXC','ARR','CCL','OFD','POD')"


def _str(col: str):
    return F.nullif(F.trim(F.col(col)), F.lit("")).alias(col)


# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #


@dp.materialized_view(name=f"{SILVER}.locations", comment="Network locations (airports, seaports, warehouses)")
@dp.expect_or_drop("valid_code", "location_code IS NOT NULL")
def locations():
    return (
        spark.read.table(f"{BRONZE}.locations")
        .select(*[_str(c) for c in ["location_code", "location_name", "city", "country_code", "region", "location_type"]])
        .dropDuplicates(["location_code"])
    )


@dp.materialized_view(name=f"{SILVER}.carriers", comment="Carrier master")
@dp.expect_or_drop("valid_carrier", "carrier_id IS NOT NULL")
@dp.expect("known_mode", "transport_mode IN ('AIR','OCEAN','ROAD')")
def carriers():
    return (
        spark.read.table(f"{BRONZE}.carriers")
        .select(*[_str(c) for c in ["carrier_id", "carrier_name", "transport_mode", "carrier_code", "country_code", "contract_type"]])
        .dropDuplicates(["carrier_id"])
    )


@dp.view(name="customers_clean")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL AND updated_at IS NOT NULL")
def customers_clean():
    return spark.readStream.table(f"{BRONZE}.customers").select(
        *[_str(c) for c in ["customer_id", "customer_name", "industry", "country_code", "city", "account_tier"]],
        F.col("credit_terms_days").cast("int").alias("credit_terms_days"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )


dp.create_streaming_table(name=f"{SILVER}.customers", comment="Customer master with SCD2 history (__START_AT/__END_AT)")
dp.create_auto_cdc_flow(
    target=f"{SILVER}.customers",
    source="customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    track_history_except_column_list=["updated_at"],
)

# --------------------------------------------------------------------------- #
# Shipments (CDC)
# --------------------------------------------------------------------------- #

SHIPMENT_RULES = {
    "has_shipment_id": "shipment_id IS NOT NULL",
    "has_customer": "customer_id IS NOT NULL",
    "positive_weight": "gross_weight_kg > 0 AND chargeable_weight_kg >= gross_weight_kg",
    "valid_route": "origin_code IS NOT NULL AND destination_code IS NOT NULL AND origin_code <> destination_code",
    "valid_dates": "planned_delivery_ts > booking_ts",
}
_ALL_SHIPMENT_RULES = " AND ".join(f"({r})" for r in SHIPMENT_RULES.values())


def _typed_shipments():
    return spark.readStream.table(f"{BRONZE}.shipments").select(
        *[_str(c) for c in [
            "shipment_id", "house_bill_number", "customer_id", "carrier_id", "transport_mode",
            "service_level", "origin_code", "destination_code", "incoterm", "commodity", "booking_status",
        ]],
        F.col("is_hazmat").cast("boolean").alias("is_hazmat"),
        F.col("pieces").cast("int").alias("pieces"),
        *[F.col(c).cast("decimal(12,2)").alias(c) for c in [
            "gross_weight_kg", "volume_cbm", "chargeable_weight_kg", "declared_value_usd",
        ]],
        *[F.to_timestamp(c).alias(c) for c in [
            "booking_ts", "planned_pickup_ts", "planned_delivery_ts", "updated_at",
        ]],
        "_source_file",
    )


@dp.view(name="shipments_clean")
@dp.expect_all_or_drop(SHIPMENT_RULES)
def shipments_clean():
    return _typed_shipments()


@dp.table(name=f"{SILVER}.shipments_quarantine", comment="Shipment records rejected by data-quality rules")
def shipments_quarantine():
    return _typed_shipments().where(f"NOT coalesce({_ALL_SHIPMENT_RULES}, false)")


dp.create_streaming_table(name=f"{SILVER}.shipments", comment="Current state of every house bill (SCD1)")
dp.create_auto_cdc_flow(
    target=f"{SILVER}.shipments",
    source="shipments_clean",
    keys=["shipment_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
)

# --------------------------------------------------------------------------- #
# Tracking, customs, billing
# --------------------------------------------------------------------------- #


@dp.table(name=f"{SILVER}.shipment_events", comment="De-duplicated tracking milestones")
@dp.expect_or_drop("has_shipment", "shipment_id IS NOT NULL")
@dp.expect_or_drop("known_event_code", f"event_code IN {EVENT_CODES}")
@dp.expect_or_drop("has_timestamp", "event_ts IS NOT NULL")
def shipment_events():
    return (
        spark.readStream.table(f"{BRONZE}.shipment_events")
        .select(
            *[_str(c) for c in ["event_id", "shipment_id", "event_code", "location_code", "remarks", "source_system"]],
            F.to_timestamp("event_ts").alias("event_ts"),
            "_ingested_at",
        )
        .withWatermark("_ingested_at", "1 day")
        .dropDuplicatesWithinWatermark(["event_id"])
    )


@dp.table(name=f"{SILVER}.customs_entries", comment="Import customs declarations")
@dp.expect_or_drop("has_shipment", "shipment_id IS NOT NULL")
@dp.expect("release_after_filing", "released_ts IS NULL OR released_ts >= filed_ts")
def customs_entries():
    return spark.readStream.table(f"{BRONZE}.customs_entries").select(
        *[_str(c) for c in ["entry_id", "shipment_id", "import_country", "broker", "status"]],
        F.to_timestamp("filed_ts").alias("filed_ts"),
        F.to_timestamp("released_ts").alias("released_ts"),
        F.col("inspection_flag").cast("boolean").alias("inspection_flag"),
        F.col("duty_amount_usd").cast("decimal(12,2)").alias("duty_amount_usd"),
    )


@dp.table(name=f"{SILVER}.invoices", comment="Customer invoices with carrier cost (buy) for margin")
@dp.expect_or_drop("has_invoice_id", "invoice_id IS NOT NULL AND shipment_id IS NOT NULL")
@dp.expect_or_drop("non_negative_total", "total_amount >= 0")
@dp.expect("total_matches_lines",
           "abs(total_amount - (freight_charge + fuel_surcharge + customs_fee + accessorial_charges)) < 0.05")
def invoices():
    return spark.readStream.table(f"{BRONZE}.invoices").select(
        *[_str(c) for c in ["invoice_id", "shipment_id", "customer_id", "currency", "payment_status"]],
        *[F.to_date(c).alias(c) for c in ["invoice_date", "due_date", "paid_date"]],
        *[F.col(c).cast("decimal(12,2)").alias(c) for c in [
            "freight_charge", "fuel_surcharge", "customs_fee", "accessorial_charges", "total_amount", "carrier_cost",
        ]],
    )
