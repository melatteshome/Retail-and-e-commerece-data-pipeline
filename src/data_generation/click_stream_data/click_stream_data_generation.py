
import argparse
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from faker import Faker


EVENT_TYPES = ["page_view", "add_to_cart", "checkout", "purchase"]
EVENT_PROBS = [0.70, 0.15, 0.10, 0.05]          # must sum to 1.0


def make_clickstream(n_rows: int,
                     customers: pd.DataFrame,
                     products: pd.DataFrame,
                     fake: Faker) -> pd.DataFrame:
    """
    Assemble a DataFrame of synthetic click‑stream events whose foreign keys
    all exist in the provided dimension tables.
    """
    customer_ids = customers["customer_id"].values
    product_ids  = products["product_id"].values

    # Vectorised random draws
    df = pd.DataFrame({
        "user_id":     np.random.choice(customer_ids, size=n_rows),
        "product_id":  np.random.choice(product_ids,  size=n_rows),
        "event_type":  np.random.choice(EVENT_TYPES, size=n_rows, p=EVENT_PROBS),
        "timestamp":   [
            fake.date_time_between(
                start_date="-30d", end_date="now", tzinfo=timezone.utc
            )
            for _ in range(n_rows)
        ],
    })

    # Optional: sort chronologically to mimic a log file
    df.sort_values("timestamp", inplace=True, ignore_index=True)
    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows",      type=int,  default=250_000)
    parser.add_argument("--seed",      type=int,  default=None)
    parser.add_argument("--customers", required=True,
                        help="Path to customers.csv (must contain customer_id)")
    parser.add_argument("--products",  required=True,
                        help="Path to products.csv  (must contain product_id)")
    parser.add_argument("--outfile",   default="clickstream.csv",
                        help="Destination CSV")
    args = parser.parse_args()

    # Reproducibility
    if args.seed is not None:
        random.seed(args.seed)
        np.random.seed(args.seed)

    fake = Faker()
    Faker.seed(args.seed)

    # Load dimensions
    customers = pd.read_csv(args.customers, usecols=["customer_id"])
    products  = pd.read_csv(args.products,  usecols=["product_id"])

    # Generate click‑stream
    clicks = make_clickstream(args.rows, customers, products, fake)

    # Persist
    clicks.to_csv(args.outfile, index=False)
    print(f"✔ {len(clicks):,} events written to {args.outfile}")


if __name__ == "__main__":
    main()
