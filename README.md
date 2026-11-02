# Freight Lakehouse: Databricks project for logistics

This is an end-to-end Databricks lakehouse for a **freight forwarder and customs broker**: air, ocean and road shipments, carrier tracking, customs clearance and billing. It is modeled on the operations of a mid-size forwarder such as EMO Trans. All data is synthetic.

## Architecture

```
 Source systems (simulated)            Unity Catalog: <catalog>
 ─────────────────────────             ──────────────────────────────────────────────────────────
 TMS bookings      (CSV)  ┐
 Carrier EDI events(JSON) │   raw.landing   bronze.*            silver.*                gold.*
 Customs filings   (JSON) ├──► (volume) ──► Auto Loader ──► typed + expectations ──► KPI materialized views
 Billing invoices  (CSV)  │                 strings only    CDC (SCD1/SCD2)           shipment_status
 CRM customers     (CSV)  ┘                                 quarantine                lane / carrier / customer
                                                                                     customs / AR aging
```

All of this runs as one serverless job, `freight-lakehouse-daily`:

| Task | What it does |
|---|---|
| `setup_catalog` | Creates the `raw`, `bronze`, `silver` and `gold` schemas and the `raw.landing` volume |
| `generate_data` | Writes one batch of synthetic files (2,000 shipments by default) |
| `refresh_lakehouse` | Runs the Lakeflow declarative pipeline `freight-lakehouse-pipeline` |
| `data_quality_report` | Shows row counts and KPIs, and fails if the quarantine rate is above 5% |

## Data model

| Layer | Table | Notes |
|---|---|---|
| silver | `customers` | SCD2: account tier and credit terms history (`__START_AT`, `__END_AT`) |
| silver | `shipments` | SCD1 via AUTO CDC: amendments and cancellations update the house bill |
| silver | `shipments_quarantine` | Records with no customer, a negative weight or a bad route |
| silver | `shipment_events` | BKD → PUP → DEP → (EXC) → ARR → CCL → OFD → POD, de-duplicated |
| silver | `customs_entries`, `invoices`, `carriers`, `locations` | Typed and validated |
| gold | `shipment_status` | One row per shipment: milestones, status, transit hours, on-time flag |
| gold | `daily_operations_kpis` | On-time delivery %, transit time and exceptions per day and mode |
| gold | `lane_performance` | Volume, service, revenue, margin and yield per kg for each lane |
| gold | `carrier_scorecard` | On-time %, exception rate and cost per kg for each carrier |
| gold | `customer_profitability` | Revenue, gross margin % and service level for each account |
| gold | `customs_clearance_kpis` | Dwell time (average and P90), inspection rate and duties |
| gold | `ar_aging` | Open receivables in buckets (current, 1-30, 31-60, 61-90, 90+ days) |

The business rules follow normal freight practice:
- **Chargeable weight** is the greater of the gross weight and the volumetric weight. The volumetric factor is 167 kg/cbm for air, 1,000 for ocean and 333 for road.
- **Revenue** is freight, fuel surcharge, customs fee and accessorials.
- **Margin** is revenue minus carrier cost (the buy rate).
- **On time** means proof of delivery happened at or before the planned delivery time.

## Project layout

```
databricks.yml                     bundle definition, dev and prod targets
resources/freight_pipeline.yml     serverless Lakeflow pipeline
resources/freight_daily_job.yml    daily serverless job (performance target STANDARD)
src/setup/                         Unity Catalog setup notebook
src/data_generator/                datagen.py (pure Python) and its notebook
src/pipeline/                      bronze.py, silver.py, gold.py
src/quality/                       data-quality report notebook
resources/freight_dashboard.yml    AI/BI dashboard on the gold tables
src/dashboards/                    freight_operations.lvdash.json (dashboard definition)
src/analytics/freight_kpis.sql     business queries for a SQL warehouse
tests/                             pytest tests for the generator
```

## Getting started

1. Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) and log in:
   ```bash
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```
2. Make sure the `logistics_dev` catalog exists, or that you can create it. To use a different catalog, pass `--var catalog=<name>`. If you saved your login under a named profile, add `--profile <name>` to the bundle commands.
3. Validate, deploy and run:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run freight_daily_job -t dev
   ```
4. Run the job again to add more batches. Each run adds new shipments and customer changes, and the pipeline processes only the new files.
5. Open the **Freight Operations Control Tower** dashboard, under Dashboards in the workspace. It has three pages:
   - **Operations control tower:** active, overdue and exception shipments, on-time %, daily deliveries, and a list of late shipments. You can filter it by transport mode.
   - **Carriers & lanes:** on-time % and spend for each carrier, and the top lanes by margin.
   - **Customers, customs & finance:** revenue, margin, receivables aging, customs clearance time and customer profitability.

   The dashboard uses the `Serverless Starter Warehouse`. To use a different warehouse, pass `--var warehouse_id=<id>`.
6. For ad-hoc analysis, use `src/analytics/freight_kpis.sql`.

Run the local tests with:
```bash
pip install -r requirements-dev.txt
pytest -q
```

## CI/CD with GitHub Actions

The workflow in `.github/workflows/ci-cd.yml` does three things:
- runs the tests on every pull request and push
- validates the bundle for both targets
- deploys to `prod` when changes are pushed to `main`

To connect it to your workspace, create a Databricks service principal and give it an OAuth secret. Then add these repository secrets under **Settings → Secrets and variables → Actions**:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://<your-workspace>.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | the service principal's application ID |
| `DATABRICKS_CLIENT_SECRET` | the service principal's OAuth secret |

Until these secrets are set, validation and deployment are skipped with a warning and only the tests run. The deploy job uses a GitHub environment called `prod`. You can add required reviewers to that environment so someone approves each production deployment.

## Ideas for extending it

- Add a Genie space so operations staff can ask questions in plain language ("which lanes were late last week?").
- Predict ETA or delay risk with ML from `shipment_status` features, tracked in MLflow.
- Stream real carrier events with Zerobus or Kafka instead of the file generator.
- Add row-level security so each customer sees only their own shipments.
