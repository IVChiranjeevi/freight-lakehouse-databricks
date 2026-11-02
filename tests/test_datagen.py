import random
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "data_generator"))
import datagen  # noqa: E402

AS_OF = datetime(2026, 11, 1, 12, 0, 0)


@pytest.fixture(scope="module")
def batch():
    customers = datagen.build_customers()
    return datagen.generate_batch("20261101120000", AS_OF, 1500, customers, random.Random(7))


def _parse(ts):
    return datetime.fromisoformat(ts)


def test_customers_are_deterministic():
    assert datagen.build_customers() == datagen.build_customers()


def test_customer_changes_alter_tier_and_timestamp():
    customers = datagen.build_customers()
    by_id = {c["customer_id"]: c for c in customers}
    for change in datagen.customer_changes(customers, random.Random(1), AS_OF):
        assert change["account_tier"] != by_id[change["customer_id"]]["account_tier"]
        assert change["updated_at"] == AS_OF.isoformat()


def test_shipment_ids_unique_per_original_booking(batch):
    originals = [s for s in batch["shipments"] if s["booking_status"] == "CONFIRMED"]
    assert len({s["shipment_id"] for s in originals}) == len(originals) == 1500


def test_chargeable_weight_at_least_gross_for_valid_records(batch):
    for s in batch["shipments"]:
        if s["gross_weight_kg"] > 0:
            assert s["chargeable_weight_kg"] >= s["gross_weight_kg"]


def test_events_are_ordered_and_not_in_future(batch):
    order = {code: i for i, code in enumerate(datagen.EVENT_CODES)}
    by_shipment = {}
    for e in batch["shipment_events"]:
        assert _parse(e["event_ts"]) <= AS_OF
        by_shipment.setdefault(e["shipment_id"], []).append(e)
    for events in by_shipment.values():
        times = [_parse(e["event_ts"]) for e in events]
        assert times == sorted(times)
        codes = [order[e["event_code"]] for e in events]
        assert codes == sorted(codes)


def test_cancelled_shipments_are_not_invoiced(batch):
    cancelled = {s["shipment_id"] for s in batch["shipments"] if s["booking_status"] == "CANCELLED"}
    assert cancelled
    assert not cancelled & {i["shipment_id"] for i in batch["invoices"]}


def test_invoice_totals_match_lines_and_follow_delivery(batch):
    pod = {e["shipment_id"]: _parse(e["event_ts"]) for e in batch["shipment_events"] if e["event_code"] == "POD"}
    for inv in batch["invoices"]:
        lines = inv["freight_charge"] + inv["fuel_surcharge"] + inv["customs_fee"] + inv["accessorial_charges"]
        assert inv["total_amount"] == pytest.approx(lines, abs=0.02)
        assert inv["shipment_id"] in pod
        assert inv["carrier_cost"] < inv["total_amount"]


def test_customs_only_for_international(batch):
    countries = {loc["location_code"]: loc["country_code"] for loc in datagen.build_locations()}
    shipments = {s["shipment_id"]: s for s in batch["shipments"]}
    assert batch["customs_entries"]
    for entry in batch["customs_entries"]:
        s = shipments[entry["shipment_id"]]
        assert countries[s["origin_code"]] != countries[s["destination_code"]]


def test_batch_contains_deliberate_quality_defects(batch):
    bad = [s for s in batch["shipments"] if s["customer_id"] is None or s["gross_weight_kg"] <= 0]
    assert 0 < len(bad) < 0.05 * len(batch["shipments"])
