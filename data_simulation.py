import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


TOTAL_EVENTS = 250000
START_DATE = datetime(2025, 1, 1)
NUM_USERS = 40000
NUM_PRODUCTS = 5000

EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]
EVENT_PROBS = [0.65, 0.20, 0.08, 0.05, 0.02]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_PROBS = [0.6, 0.3, 0.1]

COUNTRIES = ["India", "US", "UK", "Germany", "Canada"]
COUNTRY_PROBS = [0.45, 0.25, 0.10, 0.10, 0.10]


PRODUCT_IDS = np.random.zipf(a=2, size=NUM_PRODUCTS)
PRODUCT_IDS = [f"P{pid}" for pid in PRODUCT_IDS]


def generate_events():
    events = []
    current_time = START_DATE

    for _ in range(TOTAL_EVENTS):
        user_id = f"U{random.randint(1, NUM_USERS)}"
        session_id = str(uuid.uuid4())

        session_events = random.randint(3, 15)
        has_checkout = False

        for _ in range(session_events):
            event_type = random.choices(EVENT_TYPES, EVENT_PROBS)[0]

            if event_type == "purchase" and not has_checkout:
                continue
            if event_type == "checkout":
                has_checkout = True

            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": event_type,
                "product_id": random.choice(PRODUCT_IDS)
                    if event_type != "page_view" else None,
                "device": random.choices(DEVICES, DEVICE_PROBS)[0],
                "country": random.choices(COUNTRIES, COUNTRY_PROBS)[0],
                "event_timestamp": current_time.isoformat() 
            }

            events.append(event)

            current_time += timedelta(seconds=random.randint(5, 300))

            if len(events) >= TOTAL_EVENTS:
                return events

    return events

events = generate_events()
df = pd.DataFrame(events)

output_file = "events.json"
df.to_json(output_file, orient="records", lines=True)
