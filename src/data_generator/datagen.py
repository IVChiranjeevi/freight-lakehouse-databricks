"""Synthetic data generator for a freight-forwarding business (air, ocean and road).

Pure Python (no Spark) so it can be unit tested locally and run on serverless,
where files are written straight into a Unity Catalog volume.

Entities produced:
    reference : locations, carriers, customers
    batch     : shipments, shipment_events, invoices, customs_entries
"""
from __future__ import annotations

import csv
import json
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #
# (code, name, city, country, region, location_type)
LOCATIONS = [
    ("ORD", "Chicago O'Hare Intl", "Chicago", "US", "North America", "AIRPORT"),
    ("LAX", "Los Angeles Intl", "Los Angeles", "US", "North America", "AIRPORT"),
    ("JFK", "New York JFK", "New York", "US", "North America", "AIRPORT"),
    ("ATL", "Atlanta Hartsfield-Jackson", "Atlanta", "US", "North America", "AIRPORT"),
    ("DFW", "Dallas/Fort Worth Intl", "Dallas", "US", "North America", "AIRPORT"),
    ("MIA", "Miami Intl", "Miami", "US", "North America", "AIRPORT"),
    ("FRA", "Frankfurt am Main", "Frankfurt", "DE", "Europe", "AIRPORT"),
    ("AMS", "Amsterdam Schiphol", "Amsterdam", "NL", "Europe", "AIRPORT"),
    ("LHR", "London Heathrow", "London", "GB", "Europe", "AIRPORT"),
    ("HKG", "Hong Kong Intl", "Hong Kong", "HK", "Asia Pacific", "AIRPORT"),
    ("PVG", "Shanghai Pudong", "Shanghai", "CN", "Asia Pacific", "AIRPORT"),
    ("SIN", "Singapore Changi", "Singapore", "SG", "Asia Pacific", "AIRPORT"),
    ("ICN", "Seoul Incheon", "Seoul", "KR", "Asia Pacific", "AIRPORT"),
    ("DEL", "Delhi Indira Gandhi Intl", "Delhi", "IN", "Asia Pacific", "AIRPORT"),
    ("DXB", "Dubai Intl", "Dubai", "AE", "Middle East", "AIRPORT"),
    ("MEX", "Mexico City Intl", "Mexico City", "MX", "Latin America", "AIRPORT"),
    ("USLAX", "Port of Los Angeles", "Los Angeles", "US", "North America", "SEAPORT"),
    ("USNYC", "Port of New York/New Jersey", "Newark", "US", "North America", "SEAPORT"),
    ("USSAV", "Port of Savannah", "Savannah", "US", "North America", "SEAPORT"),
    ("USHOU", "Port of Houston", "Houston", "US", "North America", "SEAPORT"),
    ("NLRTM", "Port of Rotterdam", "Rotterdam", "NL", "Europe", "SEAPORT"),
    ("DEHAM", "Port of Hamburg", "Hamburg", "DE", "Europe", "SEAPORT"),
    ("CNSHA", "Port of Shanghai", "Shanghai", "CN", "Asia Pacific", "SEAPORT"),
    ("CNNGB", "Port of Ningbo-Zhoushan", "Ningbo", "CN", "Asia Pacific", "SEAPORT"),
    ("SGSIN", "Port of Singapore", "Singapore", "SG", "Asia Pacific", "SEAPORT"),
    ("KRPUS", "Port of Busan", "Busan", "KR", "Asia Pacific", "SEAPORT"),
    ("INNSA", "Nhava Sheva (JNPT)", "Mumbai", "IN", "Asia Pacific", "SEAPORT"),
    ("AEJEA", "Jebel Ali", "Dubai", "AE", "Middle East", "SEAPORT"),
    ("WH-CHI", "Chicago Distribution Center", "Elk Grove Village", "US", "North America", "WAREHOUSE"),
    ("WH-LAX", "LA Gateway Warehouse", "Carson", "US", "North America", "WAREHOUSE"),
    ("WH-NYC", "NY/NJ Cross-Dock", "Secaucus", "US", "North America", "WAREHOUSE"),
    ("WH-ATL", "Atlanta Cross-Dock", "College Park", "US", "North America", "WAREHOUSE"),
    ("WH-DFW", "Dallas Warehouse", "Grapevine", "US", "North America", "WAREHOUSE"),
    ("WH-MIA", "Miami Consolidation Hub", "Doral", "US", "North America", "WAREHOUSE"),
]

# (carrier_id, name, mode, carrier_code, country, contract_type)
CARRIERS = [
    ("CAR-001", "SkyBridge Cargo", "AIR", "SBC", "US", "CONTRACT"),
    ("CAR-002", "AeroFreight Global", "AIR", "AFG", "DE", "CONTRACT"),
    ("CAR-003", "Pacific Wings Cargo", "AIR", "PWC", "SG", "SPOT"),
    ("CAR-004", "Atlas Jet Logistics", "AIR", "AJL", "AE", "SPOT"),
    ("CAR-005", "BlueWave Lines", "OCEAN", "BWLU", "NL", "CONTRACT"),
    ("CAR-006", "Meridian Container Line", "OCEAN", "MCLU", "KR", "CONTRACT"),
    ("CAR-007", "Harbor Star Shipping", "OCEAN", "HSSU", "CN", "SPOT"),
    ("CAR-008", "Interstate Haul Co", "ROAD", "IHCO", "US", "CONTRACT"),
    ("CAR-009", "Prairie Express Trucking", "ROAD", "PXTR", "US", "SPOT"),
    ("CAR-010", "CrossDock Carriers", "ROAD", "CDCR", "US", "CONTRACT"),
]

_CUSTOMER_PREFIXES = [
    "Acme", "Summit", "Northwind", "Blue Ridge", "Granite", "Pioneer", "Evergreen",
    "Silverline", "Redwood", "Horizon", "Keystone", "Lakeshore", "Ironwood", "Sunbelt",
    "Cobalt", "Prairie", "Vertex", "Orion", "Bayview", "Copperfield",
]
# suffix -> industry
_CUSTOMER_SUFFIXES = {
    "Industries": "Industrial", "Manufacturing": "Industrial", "Electronics": "High Tech",
    "Pharma": "Life Sciences", "Medical Devices": "Life Sciences", "Apparel": "Retail",
    "Automotive": "Automotive", "Foods": "Food & Beverage", "Aerospace": "Aerospace",
    "Retail Group": "Retail",
}
_INDUSTRY_COMMODITIES = {
    "Industrial": ["Machinery", "Spare Parts", "Chemicals"],
    "High Tech": ["Electronics", "Semiconductors"],
    "Life Sciences": ["Pharmaceuticals", "Medical Devices"],
    "Retail": ["Apparel", "Consumer Goods"],
    "Automotive": ["Auto Parts", "Tires"],
    "Food & Beverage": ["Perishables", "Beverages"],
    "Aerospace": ["Aerospace Components"],
}
_CUSTOMER_LOCATIONS = [
    ("US", "Chicago"), ("US", "Los Angeles"), ("US", "Dallas"), ("US", "Atlanta"),
    ("US", "Newark"), ("US", "Miami"), ("DE", "Frankfurt"), ("NL", "Rotterdam"),
    ("CN", "Shanghai"), ("IN", "Mumbai"), ("SG", "Singapore"), ("MX", "Monterrey"),
]
ACCOUNT_TIERS = ["STRATEGIC", "KEY", "STANDARD"]
CREDIT_TERMS = [15, 30, 45, 60]

# --------------------------------------------------------------------------- #
# Business rules
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ModeProfile:
    location_type: str
    transit_days: tuple[int, int]
    weight_kg: tuple[float, float]
    rate_per_kg: tuple[float, float]
    min_charge: float
    dim_factor: float  # kg per cbm used for chargeable weight
    max_delay_hours: float


MODE_PROFILES = {
    "AIR": ModeProfile("AIRPORT", (2, 6), (25, 3_500), (3.5, 6.5), 150.0, 167.0, 96),
    "OCEAN": ModeProfile("SEAPORT", (18, 40), (1_500, 24_000), (0.12, 0.35), 450.0, 1_000.0, 240),
    "ROAD": ModeProfile("WAREHOUSE", (1, 5), (150, 18_000), (0.25, 0.75), 95.0, 333.0, 48),
}
MODE_WEIGHTS = {"AIR": 0.45, "OCEAN": 0.30, "ROAD": 0.25}
# service level -> (transit multiplier, price multiplier, weight)
SERVICE_LEVELS = {"EXPRESS": (0.7, 1.35, 0.2), "STANDARD": (1.0, 1.0, 0.6), "ECONOMY": (1.3, 0.85, 0.2)}
INCOTERMS = ["EXW", "FCA", "FOB", "CIF", "DAP", "DDP"]
CUSTOMS_BROKERS = ["In-house Brokerage", "Gateway Customs LLC", "Clearpath Brokers"]
EXCEPTION_REASONS = [
    "Weather delay", "Missed connection", "Port congestion", "Documentation discrepancy",
    "Capacity rollover", "Equipment shortage",
]
EVENT_CODES = ("BKD", "PUP", "DEP", "EXC", "ARR", "CCL", "OFD", "POD")

LATE_PROBABILITY = 0.22
CANCEL_PROBABILITY = 0.02
AMEND_PROBABILITY = 0.05
INSPECTION_PROBABILITY = 0.08
BAD_CUSTOMER_PROBABILITY = 0.015
BAD_WEIGHT_PROBABILITY = 0.01
HISTORY_DAYS = 60

REFERENCE_EPOCH = datetime(2024, 1, 1)


def _ts(dt: datetime | None) -> str | None:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") if dt else None


def _weighted(rng: random.Random, weights: dict[str, float]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()))[0]


# --------------------------------------------------------------------------- #
# Reference builders
# --------------------------------------------------------------------------- #


def build_locations() -> list[dict]:
    cols = ["location_code", "location_name", "city", "country_code", "region", "location_type"]
    return [dict(zip(cols, row)) for row in LOCATIONS]


def build_carriers() -> list[dict]:
    cols = ["carrier_id", "carrier_name", "transport_mode", "carrier_code", "country_code", "contract_type"]
    return [dict(zip(cols, row)) for row in CARRIERS]


def build_customers(n: int = 150, seed: int = 42) -> list[dict]:
    """Deterministic customer master, identical on every run for the same seed."""
    rng = random.Random(seed)
    customers = []
    for i in range(1, n + 1):
        suffix = rng.choice(list(_CUSTOMER_SUFFIXES))
        country, city = rng.choice(_CUSTOMER_LOCATIONS)
        customers.append({
            "customer_id": f"CUS-{i:05d}",
            "customer_name": f"{rng.choice(_CUSTOMER_PREFIXES)} {suffix} {i}",
            "industry": _CUSTOMER_SUFFIXES[suffix],
            "country_code": country,
            "city": city,
            "account_tier": rng.choices(ACCOUNT_TIERS, weights=[0.1, 0.3, 0.6])[0],
            "credit_terms_days": rng.choice(CREDIT_TERMS),
            "updated_at": _ts(REFERENCE_EPOCH + timedelta(days=rng.randint(0, 300))),
        })
    return customers


def customer_changes(customers: list[dict], rng: random.Random, as_of: datetime,
                     change_rate: float = 0.03) -> list[dict]:
    """Tier / credit-term changes for a sample of customers (drives SCD2 history)."""
    changes = []
    for c in rng.sample(customers, max(1, int(len(customers) * change_rate))):
        changed = dict(c)
        changed["account_tier"] = rng.choice([t for t in ACCOUNT_TIERS if t != c["account_tier"]])
        changed["credit_terms_days"] = rng.choice(CREDIT_TERMS)
        changed["updated_at"] = _ts(as_of)
        changes.append(changed)
    return changes


# --------------------------------------------------------------------------- #
# Transactional batch
# --------------------------------------------------------------------------- #


def generate_batch(batch_id: str, as_of: datetime, num_shipments: int,
                   customers: list[dict], rng: random.Random) -> dict[str, list[dict]]:
    """One ingest batch: shipments booked over the last HISTORY_DAYS, with every
    tracking event, customs entry and invoice that has happened by ``as_of``."""
    locations = build_locations()
    locs_by_type: dict[str, list[dict]] = {}
    for loc in locations:
        locs_by_type.setdefault(loc["location_type"], []).append(loc)
    carriers_by_mode: dict[str, list[dict]] = {}
    for car in build_carriers():
        carriers_by_mode.setdefault(car["transport_mode"], []).append(car)

    out: dict[str, list[dict]] = {"shipments": [], "shipment_events": [], "invoices": [], "customs_entries": []}
    for seq in range(1, num_shipments + 1):
        _generate_shipment(out, rng, f"SHP-{batch_id}-{seq:06d}", as_of, customers,
                           locs_by_type, carriers_by_mode)
    return out


def _generate_shipment(out, rng, shipment_id, as_of, customers, locs_by_type, carriers_by_mode):
    mode = _weighted(rng, MODE_WEIGHTS)
    profile = MODE_PROFILES[mode]
    origin, destination = rng.sample(locs_by_type[profile.location_type], 2)
    international = origin["country_code"] != destination["country_code"]
    carrier = rng.choice(carriers_by_mode[mode])
    customer = rng.choice(customers)
    service = rng.choices(list(SERVICE_LEVELS), weights=[s[2] for s in SERVICE_LEVELS.values()])[0]
    transit_mult, price_mult, _ = SERVICE_LEVELS[service]
    commodity = rng.choice(_INDUSTRY_COMMODITIES[customer["industry"]])

    booking = as_of - timedelta(days=rng.uniform(0, HISTORY_DAYS))
    planned_pickup = booking + timedelta(hours=rng.uniform(4, 48))
    planned_delivery = planned_pickup + timedelta(days=rng.randint(*profile.transit_days) * transit_mult)

    gross_kg = round(rng.uniform(*profile.weight_kg), 1)
    volume_cbm = round(gross_kg / rng.uniform(120, 450), 2)
    chargeable_kg = round(max(gross_kg, volume_cbm * profile.dim_factor), 1)

    shipment = {
        "shipment_id": shipment_id,
        "house_bill_number": f"HB{rng.randint(10**9, 10**10 - 1)}",
        "customer_id": customer["customer_id"],
        "carrier_id": carrier["carrier_id"],
        "transport_mode": mode,
        "service_level": service,
        "origin_code": origin["location_code"],
        "destination_code": destination["location_code"],
        "incoterm": rng.choice(INCOTERMS),
        "commodity": commodity,
        "is_hazmat": commodity == "Chemicals" or rng.random() < 0.02,
        "pieces": rng.randint(1, 60),
        "gross_weight_kg": gross_kg,
        "volume_cbm": volume_cbm,
        "chargeable_weight_kg": chargeable_kg,
        "declared_value_usd": round(gross_kg * rng.uniform(8, 120), 2),
        "booking_status": "CONFIRMED",
        "booking_ts": _ts(booking),
        "planned_pickup_ts": _ts(planned_pickup),
        "planned_delivery_ts": _ts(planned_delivery),
        "updated_at": _ts(booking),
    }

    # Data-quality defects that the pipeline expectations must catch.
    if rng.random() < BAD_CUSTOMER_PROBABILITY:
        shipment["customer_id"] = None
    if rng.random() < BAD_WEIGHT_PROBABILITY:
        shipment["gross_weight_kg"] = -abs(gross_kg)
    out["shipments"].append(shipment)

    events = out["shipment_events"]

    def emit(code, ts, loc, remarks=None):
        if ts <= as_of:
            events.append({
                "event_id": f"{shipment_id}-{code}-{len(events):07d}",
                "shipment_id": shipment_id,
                "event_code": code,
                "event_ts": _ts(ts),
                "location_code": loc["location_code"],
                "remarks": remarks,
                "source_system": f"{carrier['carrier_code']}-EDI",
            })

    emit("BKD", booking, origin)

    # Booking change records exercise CDC (latest updated_at wins in silver).
    if rng.random() < CANCEL_PROBABILITY:
        cancel_ts = min(booking + timedelta(hours=rng.uniform(1, 24)), as_of)
        out["shipments"].append({**shipment, "booking_status": "CANCELLED", "updated_at": _ts(cancel_ts)})
        return
    if rng.random() < AMEND_PROBABILITY:
        amend_ts = min(booking + timedelta(hours=rng.uniform(1, 12)), as_of)
        amended_kg = round(gross_kg * rng.uniform(0.85, 1.2), 1)
        out["shipments"].append({
            **shipment, "booking_status": "AMENDED", "gross_weight_kg": amended_kg,
            "chargeable_weight_kg": round(max(amended_kg, volume_cbm * profile.dim_factor), 1),
            "updated_at": _ts(amend_ts),
        })
        chargeable_kg = round(max(amended_kg, volume_cbm * profile.dim_factor), 1)

    inspected = international and rng.random() < INSPECTION_PROBABILITY
    delay_hours = rng.uniform(4, profile.max_delay_hours) if rng.random() < LATE_PROBABILITY else 0.0
    if inspected:
        delay_hours += rng.uniform(24, 72)

    pickup = planned_pickup + timedelta(hours=rng.uniform(-2, 6))
    delivery = planned_delivery + timedelta(hours=delay_hours + rng.uniform(-6, 0))
    delivery = max(delivery, pickup + timedelta(hours=6))
    span = delivery - pickup

    emit("PUP", pickup, origin)
    emit("DEP", pickup + span * 0.15, origin)
    if delay_hours > 24 and rng.random() < 0.6:
        emit("EXC", pickup + span * 0.40, origin, rng.choice(EXCEPTION_REASONS))
    arrival = pickup + span * 0.70
    emit("ARR", arrival, destination)
    if international:
        release = pickup + span * 0.82
        emit("CCL", release, destination, "Inspection completed" if inspected else None)
        if arrival <= as_of:
            out["customs_entries"].append({
                "entry_id": f"ENT-{shipment_id}",
                "shipment_id": shipment_id,
                "import_country": destination["country_code"],
                "broker": rng.choice(CUSTOMS_BROKERS),
                "filed_ts": _ts(arrival),
                "released_ts": _ts(release) if release <= as_of else None,
                "status": "RELEASED" if release <= as_of else "PENDING",
                "inspection_flag": inspected,
                "duty_amount_usd": round(shipment["declared_value_usd"] * rng.uniform(0.0, 0.12), 2),
            })
    emit("OFD", pickup + span * 0.93, destination)
    emit("POD", delivery, destination, "Signed by consignee")

    invoice_date = delivery + timedelta(days=1)
    if invoice_date <= as_of:
        out["invoices"].append(_build_invoice(rng, shipment, customer, profile, chargeable_kg,
                                              price_mult, international, invoice_date, as_of))


def _build_invoice(rng, shipment, customer, profile, chargeable_kg, price_mult, international,
                   invoice_date, as_of):
    freight = round(max(profile.min_charge, chargeable_kg * rng.uniform(*profile.rate_per_kg) * price_mult), 2)
    fuel = round(freight * rng.uniform(0.08, 0.18), 2)
    customs_fee = round(rng.uniform(85, 350), 2) if international else 0.0
    accessorial = round(rng.choice([0, 0, 0, 45, 75, 150]) * (2 if shipment["is_hazmat"] else 1), 2)
    total = round(freight + fuel + customs_fee + accessorial, 2)
    due_date = invoice_date + timedelta(days=customer["credit_terms_days"])

    paid_date = None
    pay_probability = 0.85 if due_date <= as_of else 0.3
    if rng.random() < pay_probability:
        candidate = invoice_date + timedelta(days=rng.uniform(1, customer["credit_terms_days"] + 10))
        paid_date = candidate if candidate <= as_of else None

    return {
        "invoice_id": f"INV-{shipment['shipment_id']}",
        "shipment_id": shipment["shipment_id"],
        "customer_id": shipment["customer_id"],
        "invoice_date": invoice_date.strftime("%Y-%m-%d"),
        "due_date": due_date.strftime("%Y-%m-%d"),
        "currency": "USD",
        "freight_charge": freight,
        "fuel_surcharge": fuel,
        "customs_fee": customs_fee,
        "accessorial_charges": accessorial,
        "total_amount": total,
        "carrier_cost": round((freight + fuel) * rng.uniform(0.68, 0.88), 2),
        "payment_status": "PAID" if paid_date else "UNPAID",
        "paid_date": paid_date.strftime("%Y-%m-%d") if paid_date else None,
    }


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #


def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
