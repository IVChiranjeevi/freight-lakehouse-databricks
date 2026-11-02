"""Gold: business-facing materialized views for operations, finance and customers."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

STATUS_BY_LAST_EVENT = {
    "BKD": "BOOKED", "PUP": "PICKED_UP", "DEP": "IN_TRANSIT", "EXC": "EXCEPTION",
    "ARR": "ARRIVED", "CCL": "CUSTOMS_CLEARED", "OFD": "OUT_FOR_DELIVERY", "POD": "DELIVERED",
}


def _first(code: str):
    return F.min(F.when(F.col("event_code") == code, F.col("event_ts")))


def _pct(numerator, denominator):
    return F.round(100 * numerator / F.nullif(denominator, F.lit(0)), 2)


@dp.materialized_view(name=f"{GOLD}.shipment_status",
                      comment="One row per shipment: milestones, current status, transit time and on-time flag")
def shipment_status():
    milestones = spark.read.table(f"{SILVER}.shipment_events").groupBy("shipment_id").agg(
        _first("PUP").alias("pickup_ts"),
        _first("DEP").alias("departure_ts"),
        _first("ARR").alias("arrival_ts"),
        _first("CCL").alias("customs_cleared_ts"),
        _first("POD").alias("delivered_ts"),
        F.max("event_ts").alias("last_event_ts"),
        F.max_by("event_code", "event_ts").alias("last_event_code"),
        F.count_if(F.col("event_code") == "EXC").alias("exception_count"),
    )
    status_map = F.create_map(*[F.lit(x) for kv in STATUS_BY_LAST_EVENT.items() for x in kv])
    locs = spark.read.table(f"{SILVER}.locations")

    return (
        spark.read.table(f"{SILVER}.shipments")
        .join(milestones, "shipment_id", "left")
        .join(locs.select(F.col("location_code").alias("origin_code"),
                          F.col("country_code").alias("origin_country"),
                          F.col("region").alias("origin_region")), "origin_code", "left")
        .join(locs.select(F.col("location_code").alias("destination_code"),
                          F.col("country_code").alias("destination_country"),
                          F.col("region").alias("destination_region")), "destination_code", "left")
        .withColumn("shipment_status",
                    F.when(F.col("booking_status") == "CANCELLED", F.lit("CANCELLED"))
                    .otherwise(F.coalesce(status_map[F.col("last_event_code")], F.lit("BOOKED"))))
        .withColumn("is_international", F.col("origin_country") != F.col("destination_country"))
        .withColumn("is_delivered", F.col("delivered_ts").isNotNull())
        .withColumn("transit_hours",
                    F.round((F.unix_timestamp("delivered_ts") - F.unix_timestamp("pickup_ts")) / 3600, 1))
        .withColumn("delay_hours",
                    F.round(F.greatest(F.lit(0), F.unix_timestamp("delivered_ts")
                                       - F.unix_timestamp("planned_delivery_ts")) / 3600, 1))
        .withColumn("is_on_time",
                    F.when(F.col("is_delivered"), F.col("delivered_ts") <= F.col("planned_delivery_ts")))
        .withColumn("is_overdue_in_transit",
                    (~F.col("is_delivered")) & (F.col("shipment_status") != "CANCELLED")
                    & (F.col("planned_delivery_ts") < F.current_timestamp()))
        .withColumn("booking_date", F.to_date("booking_ts"))
        .withColumn("delivery_date", F.to_date("delivered_ts"))
    )


@dp.materialized_view(name=f"{GOLD}.daily_operations_kpis",
                      comment="Daily delivered volume and on-time performance by transport mode")
def daily_operations_kpis():
    delivered = spark.read.table(f"{GOLD}.shipment_status").where("is_delivered")
    return delivered.groupBy("delivery_date", "transport_mode").agg(
        F.count("*").alias("shipments_delivered"),
        F.sum("chargeable_weight_kg").alias("chargeable_weight_kg"),
        _pct(F.count_if("is_on_time"), F.count("*")).alias("on_time_pct"),
        F.round(F.avg("transit_hours"), 1).alias("avg_transit_hours"),
        F.round(F.avg(F.when(~F.col("is_on_time"), F.col("delay_hours"))), 1).alias("avg_delay_hours_when_late"),
        F.sum("exception_count").alias("exceptions"),
    )


@dp.materialized_view(name=f"{GOLD}.lane_performance",
                      comment="Origin-destination lane volume, service and yield")
def lane_performance():
    status = spark.read.table(f"{GOLD}.shipment_status").where("shipment_status <> 'CANCELLED'")
    inv = spark.read.table(f"{SILVER}.invoices").select("shipment_id", "total_amount", "carrier_cost")
    return (
        status.join(inv, "shipment_id", "left")
        .groupBy("transport_mode", "origin_code", "destination_code", "origin_region", "destination_region")
        .agg(
            F.count("*").alias("shipments"),
            F.sum("chargeable_weight_kg").alias("chargeable_weight_kg"),
            _pct(F.count_if("is_on_time"), F.count_if("is_delivered")).alias("on_time_pct"),
            F.round(F.avg("transit_hours"), 1).alias("avg_transit_hours"),
            F.sum("total_amount").alias("revenue_usd"),
            F.sum(F.col("total_amount") - F.col("carrier_cost")).alias("gross_margin_usd"),
            F.round(F.sum("total_amount") / F.sum(F.when(F.col("total_amount").isNotNull(),
                                                          F.col("chargeable_weight_kg"))), 3)
            .alias("revenue_per_kg"),
        )
    )


@dp.materialized_view(name=f"{GOLD}.carrier_scorecard",
                      comment="Carrier on-time performance, exceptions and buy cost")
def carrier_scorecard():
    status = spark.read.table(f"{GOLD}.shipment_status").where("shipment_status <> 'CANCELLED'")
    inv = spark.read.table(f"{SILVER}.invoices").select("shipment_id", "carrier_cost")
    carriers = spark.read.table(f"{SILVER}.carriers").select("carrier_id", "carrier_name", "contract_type")
    return (
        status.join(inv, "shipment_id", "left")
        .groupBy("carrier_id", "transport_mode")
        .agg(
            F.count("*").alias("shipments"),
            F.count_if("is_delivered").alias("delivered"),
            _pct(F.count_if("is_on_time"), F.count_if("is_delivered")).alias("on_time_pct"),
            _pct(F.count_if(F.col("exception_count") > 0), F.count("*")).alias("exception_rate_pct"),
            F.round(F.avg("transit_hours"), 1).alias("avg_transit_hours"),
            F.sum("carrier_cost").alias("carrier_spend_usd"),
            F.round(F.sum("carrier_cost") / F.sum(F.when(F.col("carrier_cost").isNotNull(),
                                                         F.col("chargeable_weight_kg"))), 3)
            .alias("cost_per_kg"),
        )
        .join(carriers, "carrier_id", "left")
    )


@dp.materialized_view(name=f"{GOLD}.customer_profitability",
                      comment="Revenue, gross margin and service level per customer (current attributes)")
def customer_profitability():
    customers = (
        spark.read.table(f"{SILVER}.customers").where("__END_AT IS NULL")
        .select("customer_id", "customer_name", "industry", "country_code", "account_tier")
    )
    inv = spark.read.table(f"{SILVER}.invoices").groupBy("customer_id").agg(
        F.count("*").alias("invoices"),
        F.sum("total_amount").alias("revenue_usd"),
        F.sum(F.col("total_amount") - F.col("carrier_cost")).alias("gross_margin_usd"),
    )
    ops = spark.read.table(f"{GOLD}.shipment_status").groupBy("customer_id").agg(
        F.count("*").alias("shipments"),
        F.count_if(F.col("shipment_status") == "CANCELLED").alias("cancelled_shipments"),
        _pct(F.count_if("is_on_time"), F.count_if("is_delivered")).alias("on_time_pct"),
    )
    return (
        customers.join(ops, "customer_id", "left").join(inv, "customer_id", "left")
        .withColumn("gross_margin_pct", _pct(F.col("gross_margin_usd"), F.col("revenue_usd")))
        .withColumn("avg_revenue_per_shipment", F.round(F.col("revenue_usd") / F.col("invoices"), 2))
    )


@dp.materialized_view(name=f"{GOLD}.customs_clearance_kpis",
                      comment="Customs dwell time, inspection rate and duties by import country and broker")
def customs_clearance_kpis():
    entries = spark.read.table(f"{SILVER}.customs_entries").withColumn(
        "clearance_hours", (F.unix_timestamp("released_ts") - F.unix_timestamp("filed_ts")) / 3600)
    return entries.groupBy("import_country", "broker").agg(
        F.count("*").alias("entries"),
        F.count_if(F.col("status") == "PENDING").alias("pending_entries"),
        F.round(F.avg("clearance_hours"), 1).alias("avg_clearance_hours"),
        F.round(F.percentile_approx("clearance_hours", 0.9), 1).alias("p90_clearance_hours"),
        _pct(F.count_if("inspection_flag"), F.count("*")).alias("inspection_rate_pct"),
        F.sum("duty_amount_usd").alias("duties_usd"),
    )


@dp.materialized_view(name=f"{GOLD}.ar_aging",
                      comment="Open receivables by customer and aging bucket (as of refresh date)")
def ar_aging():
    days_past_due = F.datediff(F.current_date(), F.col("due_date"))
    bucket = (
        F.when(days_past_due <= 0, "CURRENT")
        .when(days_past_due <= 30, "1-30")
        .when(days_past_due <= 60, "31-60")
        .when(days_past_due <= 90, "61-90")
        .otherwise("90+")
    )
    return (
        spark.read.table(f"{SILVER}.invoices").where("payment_status <> 'PAID'")
        .withColumn("aging_bucket", bucket)
        .groupBy("customer_id", "aging_bucket")
        .agg(F.count("*").alias("open_invoices"), F.sum("total_amount").alias("open_amount_usd"))
    )
