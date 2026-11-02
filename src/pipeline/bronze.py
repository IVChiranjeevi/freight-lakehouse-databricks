"""Bronze: incremental ingestion of landing files with Auto Loader.

Every column lands as a string; typing and validation happen in silver. Each row
keeps its source file and ingestion time for lineage and replay.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_path")

SOURCES = {
    "locations": ("csv", "Airports, seaports and warehouses in the network"),
    "carriers": ("csv", "Air, ocean and road carriers"),
    "customers": ("csv", "Shipper accounts from the CRM"),
    "shipments": ("csv", "House bill bookings from the TMS, including amendments"),
    "shipment_events": ("json", "Tracking milestones from carrier EDI feeds"),
    "customs_entries": ("json", "Import customs declarations"),
    "invoices": ("csv", "Customer invoices from billing"),
}


def _define_bronze_table(entity: str, fmt: str, comment: str) -> None:
    @dp.table(
        name=f"{CATALOG}.bronze.{entity}",
        comment=comment,
        table_properties={"quality": "bronze"},
    )
    def _ingest():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.inferColumnTypes", "false")
            .option("header", "true")
            .load(f"{LANDING}/{entity}")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )


for _entity, (_fmt, _comment) in SOURCES.items():
    _define_bronze_table(_entity, _fmt, _comment)
