#!/usr/bin/env python3
import csv
import random
from datetime import datetime, timedelta

random.seed(7)

EVENT_TYPES = ["click", "view", "purchase", "signup", "refund"]
COUNTRIES = ["US", "CA", "IN", "GB", "AU"]


def main(out_path="data/events.csv", n=20000):
    start = datetime.now() - timedelta(days=30)

    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "user_id", "event_type", "event_ts", "amount", "country"])
        for i in range(n):
            user_id = f"u{random.randint(1, 2000)}"
            et = random.choice(EVENT_TYPES)
            ts = start + timedelta(minutes=random.randint(0, 60 * 24 * 30))
            amount = 0.0
            if et in ("purchase", "refund"):
                amount = round(max(0, random.gauss(60, 40)), 2)
            country = random.choice(COUNTRIES)
            w.writerow([f"e{i}", user_id, et, ts.strftime("%Y-%m-%d %H:%M:%S"), amount, country])

    print(f"Wrote {n} rows to {out_path}")


if __name__ == "__main__":
    main()
