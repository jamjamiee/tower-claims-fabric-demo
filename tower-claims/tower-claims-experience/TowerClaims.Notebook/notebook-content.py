# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "event_stream": {
# META       "known_event_streams": [
# META         {
# META           "artifact_id": "11111111-1111-4111-8111-111111111111",
# META           "stream_id": "11111111-1111-4111-8111-111111111111"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Tower Claims Command Centre
# ## Synthetic severe-weather claims generator for Microsoft Fabric Real-Time Intelligence
#
# This notebook emits synthetic insurance claim events to the custom endpoint source in
# the `TowerClaimsStream` Eventstream. It does not contain real Tower customer data,
# credentials, or connection strings. Eventstream connection details are discovered at
# runtime from the Fabric workspace using semantic-link-labs helper APIs.

# CELL ********************

import importlib.util
import subprocess
import sys

REQUIRED_PACKAGES = {
    "azure.eventhub": "azure-eventhub==5.11.5",
    "sempy_labs": "semantic-link-labs"
}

for module_name, package_name in REQUIRED_PACKAGES.items():
    if importlib.util.find_spec(module_name) is None:
        print(f"Installing {package_name} for this Fabric notebook session...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name, "--quiet"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import math
import random
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

import pandas as pd
from azure.eventhub import EventData, EventHubProducerClient
import sempy_labs.eventstream as sempy_eventstream

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Runtime configuration. Use max_events=None for continuous demo mode.
eventstream = "TowerClaimsStream"
eventstream_source_name = "CustomEndpoint-Source"
max_events: Optional[int] = 250
sleep_seconds = 1.0
batch_size = 10
random_seed = 42
weather_event_name = "Auckland Severe Rain and Wind Event"
weather_event_id = "WEA-AKL-2026-08"

# Synthetic reference values used by the generator.
PRODUCTS = ["House", "Contents", "Motor", "Travel"]
CHANNELS = ["My Tower", "Phone", "Broker"]
STATUSES = ["Lodged", "Validated", "Assessment", "Repair", "Settled", "Rejected"]
CLAIM_TYPES = {
    "House": ["Roof Damage", "Flood Ingress", "Fallen Tree", "Window Damage", "Retaining Wall"],
    "Contents": ["Water Damaged Contents", "Electrical Damage", "Temporary Accommodation", "Food Spoilage"],
    "Motor": ["Storm Impact", "Flood Damage", "Windscreen Damage", "Towing Required"],
    "Travel": ["Trip Disruption", "Delayed Baggage", "Cancellation", "Medical Assistance"]
}
REGIONS = {
    "Auckland Central": ["Ponsonby", "Mount Eden", "Grey Lynn", "Parnell", "Epsom"],
    "Auckland West": ["Henderson", "Te Atatu", "Glen Eden", "New Lynn", "Titirangi"],
    "Auckland North": ["Takapuna", "Albany", "Birkenhead", "Devonport", "Glenfield"],
    "Auckland South": ["Manukau", "Papakura", "Mangere", "Takanini", "Flat Bush"],
    "Auckland East": ["Howick", "Pakuranga", "Botany", "Panmure", "Beachlands"],
    "Northland": ["Whangarei", "Kerikeri", "Dargaville"],
    "Waikato": ["Hamilton", "Cambridge", "Te Awamutu"],
    "Wellington": ["Lower Hutt", "Porirua", "Wellington Central"]
}
SUPPLIERS = {
    "House": ["Auckland Building Response", "Harbour Roofing", "Kiwi Drying Services", "Rapid Make Safe"],
    "Contents": ["ContentsCare NZ", "Auckland Restoration", "Kiwi Drying Services"],
    "Motor": ["Metro Panelbeaters", "Auckland Windscreens", "Tow Assist NZ"],
    "Travel": ["Global Assist", "Travel Claims Support"]
}
ASSESSORS = ["Aroha Patel", "James Wilson", "Mereana Clark", "Liam Chen", "Sofia Brown", "Noah Thompson"]
SEVERITY_WEIGHTS = ["Low", "Medium", "High", "Critical"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

@dataclass
class TowerClaimsGenerator:
    """Generate realistic but fully synthetic Tower claims operations events."""

    seed: int = 42
    max_events: Optional[int] = None
    start_time: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.random = random.Random(self.seed)
        self.sequence = 0
        if self.start_time is None:
            self.start_time = datetime.now(timezone.utc) - timedelta(hours=2)
        self.current_time = self.start_time

    def _weighted_choice(self, values: Iterable[str], weights: Iterable[float]) -> str:
        return self.random.choices(list(values), weights=list(weights), k=1)[0]

    def _event_intensity(self) -> float:
        """Create a storm spike that peaks mid-run and then tapers."""
        if self.max_events is not None:
            progress = min(self.sequence / max(self.max_events, 1), 1)
        else:
            progress = (self.sequence % 240) / 240
        return 0.15 + 0.85 * math.exp(-((progress - 0.45) ** 2) / 0.025)

    def _product(self, weather_probability: float) -> str:
        if self.random.random() < weather_probability:
            return self._weighted_choice(PRODUCTS, [0.56, 0.24, 0.17, 0.03])
        return self._weighted_choice(PRODUCTS, [0.34, 0.22, 0.28, 0.16])

    def _region(self, weather_probability: float) -> str:
        regions = list(REGIONS.keys())
        if self.random.random() < weather_probability:
            weights = [0.22, 0.24, 0.18, 0.21, 0.11, 0.02, 0.01, 0.01]
        else:
            weights = [0.18, 0.16, 0.16, 0.15, 0.12, 0.07, 0.08, 0.08]
        return self._weighted_choice(regions, weights)

    def _estimated_cost(self, product: str, claim_type: str, severity: str, weather_claim: bool) -> float:
        product_base = {"House": 9000, "Contents": 2800, "Motor": 4200, "Travel": 1800}[product]
        severity_multiplier = {"Low": 0.45, "Medium": 0.9, "High": 1.65, "Critical": 3.1}[severity]
        weather_multiplier = 1.35 if weather_claim else 1.0
        if claim_type in {"Flood Ingress", "Retaining Wall", "Flood Damage", "Temporary Accommodation"}:
            weather_multiplier += 0.45
        noise = self.random.uniform(0.75, 1.35)
        return round(product_base * severity_multiplier * weather_multiplier * noise, 2)

    def generate_event(self) -> Dict[str, object]:
        self.sequence += 1
        spike = self._event_intensity()
        weather_claim = self.random.random() < spike
        product = self._product(spike)
        claim_type = self.random.choice(CLAIM_TYPES[product])
        region = self._region(spike)
        suburb = self.random.choice(REGIONS[region])
        severity = self._weighted_choice(SEVERITY_WEIGHTS, [0.24, 0.42, 0.25, 0.09] if weather_claim else [0.46, 0.34, 0.16, 0.04])
        status = self._weighted_choice(STATUSES, [0.22, 0.16, 0.24, 0.18, 0.16, 0.04])
        channel = self._weighted_choice(CHANNELS, [0.62, 0.27, 0.11] if weather_claim else [0.49, 0.34, 0.17])
        days_open = max(0, int(self.random.triangular(0, 28, 5 if weather_claim else 9)))
        sla_days = {"House": 10, "Contents": 7, "Motor": 8, "Travel": 5}[product]
        estimated_cost = self._estimated_cost(product, claim_type, severity, weather_claim)
        actual_cost = round(estimated_cost * self.random.uniform(0.85, 1.18), 2) if status in {"Settled", "Rejected"} else None
        days_to_settle = days_open if status == "Settled" else None
        assigned_supplier = self.random.choice(SUPPLIERS[product]) if status in {"Assessment", "Repair", "Settled"} else None
        assessor = self.random.choice(ASSESSORS) if status in {"Validated", "Assessment", "Repair", "Settled", "Rejected"} else None
        contact_count = self.random.randint(1, 3) + (1 if days_open > sla_days else 0) + (1 if severity in {"High", "Critical"} else 0)
        digital_lodgement = channel == "My Tower"
        manual_intervention_required = (
            severity == "Critical"
            or estimated_cost > 20000
            or channel != "My Tower"
            or claim_type in {"Retaining Wall", "Medical Assistance"}
        )
        sla_at_risk = status not in {"Settled", "Rejected"} and days_open >= max(sla_days - 2, 1)
        customer_satisfaction = None
        if status == "Settled":
            base_score = 8.4 if digital_lodgement else 7.4
            penalty = 1.4 if days_open > sla_days else 0
            customer_satisfaction = round(max(1, min(10, self.random.gauss(base_score - penalty, 0.9))), 1)

        self.current_time += timedelta(seconds=self.random.randint(15, 120 if weather_claim else 300))
        lodgement_time = self.current_time - timedelta(days=days_open, hours=self.random.randint(0, 23))

        return {
            "claim_id": f"CLM-{self.sequence:06d}",
            "policy_id": f"POL-{self.random.randint(100000, 999999)}",
            "customer_id": f"SYN-CUST-{self.random.randint(100000, 999999)}",
            "event_id": str(uuid.uuid4()),
            "event_sequence": self.sequence,
            "product": product,
            "claim_type": claim_type,
            "status": status,
            "lodgement_channel": channel,
            "region": region,
            "suburb": suburb,
            "country": "New Zealand",
            "supplier": assigned_supplier,
            "assessor": assessor,
            "weather_event_id": weather_event_id if weather_claim else None,
            "weather_event_name": weather_event_name if weather_claim else None,
            "weather_severity": severity if weather_claim else None,
            "rainfall_mm_24h": round(self.random.uniform(45, 170) if weather_claim else self.random.uniform(0, 35), 1),
            "wind_gust_kmh": round(self.random.uniform(65, 135) if weather_claim else self.random.uniform(5, 55), 1),
            "estimated_cost": estimated_cost,
            "actual_cost": actual_cost,
            "excess_amount": round(self.random.choice([250, 400, 500, 750, 1000]), 2),
            "reserve_amount": round(estimated_cost * self.random.uniform(1.0, 1.25), 2),
            "sla_days": sla_days,
            "days_open": days_open,
            "days_to_settle": days_to_settle,
            "contact_count": contact_count,
            "customer_satisfaction": customer_satisfaction,
            "is_weather_event": weather_claim,
            "sla_at_risk": sla_at_risk,
            "manual_intervention_required": manual_intervention_required,
            "digital_lodgement": digital_lodgement,
            "lodged_at": lodgement_time.isoformat(),
            "updated_at": self.current_time.isoformat(),
            "timestamp": self.current_time.isoformat(),
            "source_system": "Synthetic Tower Claims Generator",
            "demo_notice": "Synthetic demo data only - not an official Tower Insurance system"
        }

    def generate_batch(self, count: int) -> List[Dict[str, object]]:
        return [self.generate_event() for _ in range(count)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Discover the Eventstream custom endpoint connection. If this cell fails, verify that
# the Eventstream exists in the current workspace and that the source name matches.
es_topology = sempy_eventstream.get_eventstream_topology(eventstream=eventstream)
es_source = es_topology[es_topology["Eventstream Source Name"] == eventstream_source_name]
if es_source.empty:
    raise ValueError(f"Could not find Eventstream source '{eventstream_source_name}' in '{eventstream}'.")

es_source_id = es_source["Eventstream Source Id"].iloc[0]
es_source_connection = sempy_eventstream.get_eventstream_source_connection(
    eventstream=eventstream,
    source_id=es_source_id
)
es_eventhub_name = es_source_connection["EventHub Name"].iloc[0]
es_eventhub_connstring = es_source_connection["Primary Connection String"].iloc[0]

producer = EventHubProducerClient.from_connection_string(
    conn_str=es_eventhub_connstring,
    eventhub_name=es_eventhub_name
)

print(f"Connected to Eventstream source '{eventstream_source_name}' from host {socket.gethostname()}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Stream synthetic events. In continuous mode, stop the notebook cell to end streaming.
generator = TowerClaimsGenerator(seed=random_seed, max_events=max_events)
events: List[Dict[str, object]] = []
event_count = 0

print("Streaming synthetic Tower claims events to Eventstream...")
print(f"Configuration: max_events={max_events}, batch_size={batch_size}, sleep_seconds={sleep_seconds}")

try:
    while max_events is None or event_count < max_events:
        remaining = None if max_events is None else max_events - event_count
        current_batch_size = batch_size if remaining is None else min(batch_size, remaining)
        batch_events = generator.generate_batch(current_batch_size)
        event_batch = producer.create_batch()

        for event in batch_events:
            event_batch.add(EventData(json.dumps(event, default=str)))

        producer.send_batch(event_batch)
        events.extend(batch_events)
        event_count += len(batch_events)

        # Keep a rolling sample for the final notebook display.
        if len(events) > 500:
            events = events[-500:]

        weather_count = sum(1 for event in batch_events if event["is_weather_event"])
        sla_risk_count = sum(1 for event in batch_events if event["sla_at_risk"])
        print(
            f"Sent {event_count} events | "
            f"weather={weather_count}/{len(batch_events)} | "
            f"sla_at_risk={sla_risk_count}/{len(batch_events)}"
        )
        time.sleep(sleep_seconds)
finally:
    producer.close()

# Latest snapshot as a DataFrame, similar to the banking sample.
df = pd.DataFrame(events)
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
