# Databricks notebook source
# MAGIC %md
# MAGIC # Set up Unity Catalog objects
# MAGIC Creates the medallion schemas and the landing volume. Idempotent.
# MAGIC The catalog must already exist (or the job's run-as identity needs `CREATE CATALOG`).

# COMMAND ----------

dbutils.widgets.text("catalog", "logistics_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")

schemas = {
    "raw": "Landing zone: files dropped by source systems (TMS, carrier EDI, billing, customs)",
    "bronze": "Raw ingested records, append-only, all columns as strings",
    "silver": "Cleaned, typed, de-duplicated and conformed entities",
    "gold": "Business KPIs for operations, finance and customer reporting",
}
for schema, comment in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT '{comment}'")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`raw`.`landing` "
    "COMMENT 'Inbound files picked up by Auto Loader'"
)

print(f"Catalog {catalog} ready: schemas {', '.join(schemas)} and volume raw.landing")
