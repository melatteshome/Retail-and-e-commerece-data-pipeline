#!/usr/bin/env python
"""
Generate synthetic e‑commerce data and save to CSV.
Usage:  python gen_ecommerce.py --rows 50_000 --seed 42
"""

import argparse
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker


def make_products(n: int, fake: Faker) -> pd.DataFrame:
    categories = ["Electronics", "Home", "Fashion", "Sports", "Beauty"]
    df = pd.DataFrame(
        {
            "product_id": range(1, n + 1),
            "name": [fake.unique.catch_phrase() for _ in range(n)],
            "category": np.random.choice(categories, size=n),
            "unit_price": np.round(np.random.uniform(5.0, 999.0, size=n), 2),
            "inventory_qty": np.random.randint(0, 2_000, size=n),
        }
    )
    return df


def make_customers(n: int, fake: Faker) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "customer_id": range(1, n + 1),
            "first_name": [fake.first_name() for _ in range(n)],
            "last_name": [fake.last_name() for _ in range(n)],
            "email": [fake.unique.email() for _ in range(n)],
            "phone": [fake.phone_number() for _ in range(n)],
            "country": [fake.country_code() for _ in range(n)],
            "signup_date": [
                fake.date_between(start_date="-4y", end_date="today") for _ in range(n)
            ],
        }
    )
    return df


def make_promos(n: int, fake: Faker) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "promo_code": [fake.unique.bothify(text="????-##") for _ in range(n)],
            "description": [fake.sentence(nb_words=6) for _ in range(n)],
            "discount_pct": np.random.choice([5, 10, 15, 20, 25, 30], size=n),
            "start_date": [
                fake.date_between(start_date="-2y", end_date="-1y") for _ in range(n)
            ],
        }
    )
    df["end_date"] = df["start_date"] + pd.to_timedelta(
        np.random.randint(15, 90, size=n), unit="D"
    )
    return df


def make_transactions(
    n: int,
    fake: Faker,
    products: pd.DataFrame,
    customers: pd.DataFrame,
    promos: pd.DataFrame,
) -> pd.DataFrame:
    product_ids = products["product_id"].values
    customer_ids = customers["customer_id"].values
    promo_codes = promos["promo_code"].values

    dates = [
        fake.date_time_between(start_date="-365d", end_date="now") for _ in range(n)
    ]
    df = pd.DataFrame(
        {
            "transaction_id": range(1, n + 1),
            "customer_id": np.random.choice(customer_ids, size=n),
            "product_id": np.random.choice(product_ids, size=n),
            "promo_code": np.random.choice(
                np.append(promo_codes, [None] * 4),  # 80 % of orders have no promo
                size=n,
            ),
            "quantity": np.random.randint(1, 5, size=n),
            "transaction_timestamp": dates,
        }
    )

    # Add pricing fields (calculated, not random)
    prod_price_map = products.set_index("product_id")["unit_price"].to_dict()
    promo_map = promos.set_index("promo_code")["discount_pct"].to_dict()

    df["unit_price"] = df["product_id"].map(prod_price_map)
    df["discount_pct"] = df["promo_code"].map(promo_map).fillna(0).astype(int)
    df["gross_amount"] = df["unit_price"] * df["quantity"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"] / 100.0)

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000, help="transaction rows")
    parser.add_argument("--seed", type=int, default=None, help="random seed")
    parser.add_argument("--outdir", default="output", help="folder for CSVs")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        np.random.seed(args.seed)

    fake = Faker()
    Faker.seed(args.seed)

    # Tune entity sizes relative to transactions
    n_tx = args.rows
    n_products = max(100, int(n_tx * 0.01))
    n_customers = max(200, int(n_tx * 0.05))
    n_promos = 50

    products = make_products(n_products, fake)
    customers = make_customers(n_customers, fake)
    promos = make_promos(n_promos, fake)
    tx = make_transactions(n_tx, fake, products, customers, promos)

    # Wide view for BI demos
    tx_wide = (
        tx.merge(products, on="product_id", how="left")
        .merge(customers, on="customer_id", how="left", suffixes=("", "_cust"))
        .merge(promos[["promo_code", "discount_pct"]], on="promo_code", how="left")
    )

    # Persist
    args.outdir = Path(args.outdir)
    args.outdir.mkdir(parents=True, exist_ok=True)

    products.to_csv(args.outdir / "products.csv", index=False)
    customers.to_csv(args.outdir / "customers.csv", index=False)
    promos.to_csv(args.outdir / "promos.csv", index=False)
    tx.to_csv(args.outdir / "transactions.csv", index=False)
    tx_wide.to_csv(args.outdir / "transactions_wide.csv", index=False)

    print(f"✔ Data saved to {args.outdir.resolve()}")


if __name__ == "__main__":
    from pathlib import Path

    main()
