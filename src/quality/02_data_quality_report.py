# Databricks notebook source
# MAGIC %md
# MAGIC # Data-quality report
# MAGIC Row counts per layer, quarantine rate and headline KPIs. Fails the job if gold is empty
# MAGIC or more than 5% of shipment records were quarantined.

# COMMAND ----------

dbutils.widgets.text("catalog", "logistics_dev")
catalog = dbutils.widgets.get("catalog")
MAX_QUARANTINE_PCT = 5.0

# COMMAND ----------

from pyspark.sql import functions as F

tables = {
    "bronze": ["shipments", "shipment_events", "customs_entries", "invoices", "customers"],
    "silver": ["shipments", "shipments_quarantine", "shipment_events", "customs_entries", "invoices", "customers"],
    "gold": ["shipment_status", "daily_operations_kpis", "lane_performance", "carrier_scorecard",
             "customer_profitability", "customs_clearance_kpis", "ar_aging"],
}
counts = [(layer, t, spark.table(f"{catalog}.{layer}.{t}").count()) for layer, ts in tables.items() for t in ts]
display(spark.createDataFrame(counts, "layer string, table string, row_count long"))

# COMMAND ----------

count = {(layer, t): n for layer, t, n in counts}
bronze_shipments = count[("bronze", "shipments")]
quarantined = count[("silver", "shipments_quarantine")]
quarantine_pct = 100 * quarantined / bronze_shipments if bronze_shipments else 0.0

kpis = spark.table(f"{catalog}.gold.shipment_status").agg(
    F.count("*").alias("shipments"),
    F.count_if("is_delivered").alias("delivered"),
    F.round(100 * F.count_if("is_on_time") / F.count_if("is_delivered"), 2).alias("on_time_pct"),
    F.count_if("is_overdue_in_transit").alias("overdue_in_transit"),
)
display(kpis)
print(f"Quarantined shipment records: {quarantined:,} of {bronze_shipments:,} ({quarantine_pct:.2f}%)")

# COMMAND ----------

assert count[("gold", "shipment_status")] > 0, "gold.shipment_status is empty"
assert quarantine_pct <= MAX_QUARANTINE_PCT, (
    f"Quarantine rate {quarantine_pct:.2f}% exceeds {MAX_QUARANTINE_PCT}% threshold"
)
