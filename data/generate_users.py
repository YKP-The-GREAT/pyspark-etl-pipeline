#!/usr/bin/env python3
import csv
import random

random.seed(7)

TIERS = ["free", "pro", "enterprise"]


def main(out_path="data/users.csv", n_users=2000):
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "plan_tier"])
        for i in range(1, n_users + 1):
            plan = random.choices(TIERS, weights=[0.75, 0.2, 0.05], k=1)[0]
            w.writerow([f"u{i}", plan])

    print(f"Wrote {n_users} users to {out_path}")


if __name__ == "__main__":
    main()
