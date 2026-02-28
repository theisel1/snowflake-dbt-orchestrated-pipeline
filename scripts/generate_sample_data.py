from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

BOROUGHS = [
    "Manhattan",
    "Brooklyn",
    "Queens",
    "Bronx",
    "Staten Island",
]
VENDORS = ["V001", "V002", "V003", "V004"]
PAYMENT_TYPES = ["Card", "Cash", "Other"]


def generate_rows(row_count: int, seed: int) -> list[dict[str, str]]:
    rng = random.Random(seed)
    start_ts = datetime(2024, 1, 1, 0, 0, 0)
    rows: list[dict[str, str]] = []

    for idx in range(1, row_count + 1):
        pickup_offset_minutes = rng.randint(0, 180 * 24 * 60)
        pickup_ts = start_ts + timedelta(minutes=pickup_offset_minutes)

        trip_minutes = rng.randint(4, 75)
        dropoff_ts = pickup_ts + timedelta(minutes=trip_minutes)

        vendor_id = rng.choices(VENDORS, weights=[0.35, 0.30, 0.20, 0.15], k=1)[0]
        passenger_count = rng.choices(
            [1, 2, 3, 4, 5, 6], weights=[0.45, 0.3, 0.12, 0.08, 0.03, 0.02], k=1
        )[0]
        trip_distance = round(max(rng.gammavariate(2.2, 1.8), 0.2), 2)

        fare_amount = round(
            max(
                2.5
                + (trip_distance * 2.1)
                + (trip_minutes * 0.32)
                + rng.uniform(-1.0, 2.0),
                2.5,
            ),
            2,
        )
        payment_type = rng.choices(PAYMENT_TYPES, weights=[0.72, 0.24, 0.04], k=1)[0]

        if payment_type == "Card":
            tip_amount = round(fare_amount * rng.uniform(0.08, 0.30), 2)
        elif payment_type == "Cash":
            tip_amount = round(fare_amount * rng.uniform(0.0, 0.07), 2)
        else:
            tip_amount = round(fare_amount * rng.uniform(0.0, 0.12), 2)

        total_amount = round(fare_amount + tip_amount + rng.uniform(0.0, 2.75), 2)

        pickup_borough = rng.choices(
            BOROUGHS, weights=[0.34, 0.27, 0.22, 0.12, 0.05], k=1
        )[0]
        dropoff_borough = rng.choices(
            BOROUGHS, weights=[0.31, 0.28, 0.23, 0.12, 0.06], k=1
        )[0]

        load_ts = dropoff_ts + timedelta(minutes=rng.randint(20, 3 * 24 * 60))

        rows.append(
            {
                "trip_id": f"T{idx:08d}",
                "pickup_ts": pickup_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "dropoff_ts": dropoff_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "vendor_id": vendor_id,
                "passenger_count": str(passenger_count),
                "trip_distance": f"{trip_distance:.2f}",
                "fare_amount": f"{fare_amount:.2f}",
                "tip_amount": f"{tip_amount:.2f}",
                "total_amount": f"{total_amount:.2f}",
                "pickup_borough": pickup_borough,
                "dropoff_borough": dropoff_borough,
                "payment_type": payment_type,
                "load_ts": load_ts.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    return rows


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "trip_id",
        "pickup_ts",
        "dropoff_ts",
        "vendor_id",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "pickup_borough",
        "dropoff_borough",
        "payment_type",
        "load_ts",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate reproducible trips sample data"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10000,
        help="Number of rows to generate (default: 10000)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/sample_trips.csv"),
        help="Output CSV path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows < 1:
        raise ValueError("--rows must be >= 1")

    rows = generate_rows(row_count=args.rows, seed=args.seed)
    write_csv(rows=rows, output_path=args.output)
    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
