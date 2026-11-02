# Databricks notebook source
# MAGIC %md
# MAGIC # Generate freight-forwarding data
# MAGIC Writes one batch of synthetic files into the landing volume
# MAGIC `/Volumes/<catalog>/raw/landing/<entity>/`, where Auto Loader picks them up.
# MAGIC
# MAGIC | Entity | Format | Notes |
# MAGIC |---|---|---|
# MAGIC | locations, carriers | CSV | written once |
# MAGIC | customers | CSV | full load on first run, tier/credit changes afterwards (SCD2) |
# MAGIC | shipments | CSV | bookings + amendments/cancellations (CDC) |
# MAGIC | shipment_events | JSON | tracking milestones BKD → PUP → DEP → ARR → CCL → OFD → POD |
# MAGIC | customs_entries | JSON | import declarations for international moves |
# MAGIC | invoices | CSV | billed after proof of delivery |

# COMMAND ----------

dbutils.widgets.text("catalog", "logistics_dev")
dbutils.widgets.text("num_shipments", "2000")

catalog = dbutils.widgets.get("catalog")
num_shipments = int(dbutils.widgets.get("num_shipments"))
landing = f"/Volumes/{catalog}/raw/landing"

# COMMAND ----------

import os
import random
from datetime import datetime, timezone

import datagen

as_of = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
batch_id = as_of.strftime("%Y%m%d%H%M%S")
rng = random.Random(int(batch_id))


def _has_files(entity: str) -> bool:
    path = f"{landing}/{entity}"
    return os.path.isdir(path) and any(not f.startswith((".", "_")) for f in os.listdir(path))


written = {}

if not _has_files("locations"):
    written["locations"] = datagen.build_locations()
    datagen.write_csv(f"{landing}/locations/locations_{batch_id}.csv", written["locations"])
if not _has_files("carriers"):
    written["carriers"] = datagen.build_carriers()
    datagen.write_csv(f"{landing}/carriers/carriers_{batch_id}.csv", written["carriers"])

customers = datagen.build_customers()
written["customers"] = (
    datagen.customer_changes(customers, rng, as_of) if _has_files("customers") else customers
)
datagen.write_csv(f"{landing}/customers/customers_{batch_id}.csv", written["customers"])

batch = datagen.generate_batch(batch_id, as_of, num_shipments, customers, rng)
datagen.write_csv(f"{landing}/shipments/shipments_{batch_id}.csv", batch["shipments"])
datagen.write_jsonl(f"{landing}/shipment_events/shipment_events_{batch_id}.json", batch["shipment_events"])
datagen.write_jsonl(f"{landing}/customs_entries/customs_entries_{batch_id}.json", batch["customs_entries"])
datagen.write_csv(f"{landing}/invoices/invoices_{batch_id}.csv", batch["invoices"])
written.update(batch)

# COMMAND ----------

print(f"Batch {batch_id} written to {landing}")
for entity, rows in written.items():
    print(f"  {entity:<16} {len(rows):>7,} rows")
