"""RetailIQ source-data generator.

Generates realistic, reproducible synthetic retail data for all seven PRD
sources (§9), then deliberately injects a small percentage of data-quality
issues so the pipeline's validation layer has something real to catch during
the demo.

Outputs (all in data/raw/):
    sales.csv, returns.csv,
    customers_oracle_extract.csv, products_mysql_extract.csv,
    stores_mysql_extract.csv, suppliers_mysql_extract.csv,
    promotions_mysql_extract.csv, _injection_manifest.json

The manifest is the "answer key" for the demo (it is NOT read by the
pipeline). Deterministic: same seed -> same data -> same issues.

Usage:
    python -m scripts.generate_source_data [--n-sales 60000] [--seed 42]
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd

from config.settings import RAW_DATA_DIR

SEED = 42

# ------------------------------------------------------------------ names --
FIRST = ["Aarav", "Vivaan", "Aditya", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan",
         "Rohan", "Kabir", "Ananya", "Diya", "Aadhya", "Myra", "Sara", "Ira",
         "Priya", "Rahul", "Sneha", "Vikram", "Neha", "Amit", "Pooja", "Karan",
         "Divya", "Suresh", "Meera", "Rajesh", "Anita", "Manoj", "Kavya", "Nikhil"]
LAST = ["Sharma", "Verma", "Patel", "Iyer", "Reddy", "Nair", "Gupta", "Mehta",
        "Khan", "Singh", "Das", "Kulkarni", "Joshi", "Chopra", "Agarwal", "Menon"]

CITIES = [("Delhi", "Delhi", "North"), ("Mumbai", "Maharashtra", "West"),
          ("Bengaluru", "Karnataka", "South"), ("Chennai", "Tamil Nadu", "South"),
          ("Kolkata", "West Bengal", "East"), ("Hyderabad", "Telangana", "South"),
          ("Pune", "Maharashtra", "West"), ("Ahmedabad", "Gujarat", "West"),
          ("Jaipur", "Rajasthan", "North"), ("Lucknow", "Uttar Pradesh", "North"),
          ("Bhubaneswar", "Odisha", "East"), ("Guwahati", "Assam", "East")]

CATEGORIES = {
    "Electronics": (["Mobile", "Laptop", "Audio", "Wearables"],
                    ["boAt", "Noise", "Mi", "HP", "Samsung"]),
    "Apparel": (["Men", "Women", "Kids"], ["FabIndia", "W", "Manyavar", "Max"]),
    "Home & Kitchen": (["Cookware", "Furniture", "Decor"], ["Prestige", "Nilkamal", "Wakefit"]),
    "Grocery": (["Staples", "Snacks", "Beverages"], ["Aashirvaad", "Tata", "Parle"]),
    "Beauty": (["Skincare", "Haircare", "Cosmetics"], ["Lakme", "Himalaya", "Mamaearth"]),
    "Sports": (["Fitness", "Outdoor", "Cricket"], ["Cosco", "Nivia", "Decathlon"]),
    "Footwear": (["Casual", "Sports", "Formal"], ["Bata", "Campus", "Red Tape"]),
    "Toys": (["Educational", "Action Figures", "Board Games"], ["Funskool", "Hamleys", "Chhota Bheem"]),
}

SEGMENTS = ["Premium", "Regular", "Budget", "New"]
PROMO_TYPES = ["Festive Sale", "Clearance", "BOGO", "Loyalty Reward", "New Launch"]
CHANNELS = ["App", "In-Store", "Website", "WhatsApp", "Email"]
RETURN_REASONS = ["Defective", "Wrong Size", "Changed Mind", "Damaged in Transit",
                  "Not as Described", "Late Delivery"]

START = date(2024, 1, 1)
END = date(2025, 12, 31)


# --------------------------------------------------------------- helpers --
def _rng(seed: int = SEED):
    random.seed(seed)
    np.random.seed(seed)


def _seasonality(d: date) -> float:
    """Demand multiplier: Diwali (Oct-Nov) + year-end peaks, monsoon dip."""
    m = d.month
    mult = 1.0
    if m in (10, 11):
        mult = 1.65          # festive season
    elif m == 12:
        mult = 1.35
    elif m in (6, 7):
        mult = 0.85          # monsoon dip
    if d.weekday() >= 5:     
        mult *= 1.25
    return mult


# --------------------------------------------------------------- masters --
def gen_suppliers(n: int = 12) -> pd.DataFrame:
    regions = ["North", "South", "East", "West"]
    rows = [{
        "supplier_id": f"S{1000 + i}",
        "supplier_name": f"{random.choice(LAST)} Enterprises {i+1}",
        "supplier_region": random.choice(regions),
        "supplier_rating": round(random.uniform(3.0, 5.0), 1),
    } for i in range(n)]
    return pd.DataFrame(rows)


def gen_products(suppliers: pd.DataFrame, n: int = 240) -> pd.DataFrame:
    rows = []
    cats = list(CATEGORIES.items())
    for i in range(n):
        cat, (subs, brands) = cats[i % len(cats)]
        cost = round(random.uniform(80, 45000), 2)
        margin = random.uniform(0.18, 0.65)
        price = round(cost * (1 + margin), 2)
        rows.append({
            "product_id": f"P{10000 + i}",
            "product_name": f"{random.choice(brands)} {random.choice(subs)} {1000 + i}",
            "category": cat,
            "subcategory": random.choice(subs),
            "brand": random.choice(brands),
            "supplier_id": random.choice(suppliers["supplier_id"].tolist()),
            "unit_cost": cost,
            "selling_price": price,
        })
    return pd.DataFrame(rows)


def gen_stores(n: int = 20) -> pd.DataFrame:
    rows = []
    for i in range(n):
        city, state, region = CITIES[i % len(CITIES)]
        rows.append({
            "store_id": f"ST{100 + i}",
            "store_name": f"RetailIQ {city} {'Mall' if i % 3 == 0 else 'High Street'} {i+1}",
            "store_type": random.choice(["Flagship", "Standard", "Express"]),
            "city": city, "state": state, "region": region,
            "manager": f"{random.choice(FIRST)} {random.choice(LAST)}",
            "opening_date": (START - timedelta(days=random.randint(200, 2000))).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_customers(n: int = 8000) -> pd.DataFrame:
    rows = []
    for i in range(n):
        city, state, _ = random.choice(CITIES)
        dob = START - timedelta(days=random.randint(18 * 365, 70 * 365))
        reg = START - timedelta(days=random.randint(0, 700))
        seg = random.choices(SEGMENTS, weights=[0.12, 0.45, 0.33, 0.10])[0]
        fn, ln = random.choice(FIRST), random.choice(LAST)
        rows.append({
            "customer_id": f"C{100000 + i}",
            "name": f"{fn} {ln}",
            "gender": random.choice(["Male", "Female"]),
            "date_of_birth": dob.isoformat(),
            "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
            "phone": f"9{random.randint(100000000, 999999999)}",
            "city": city, "state": state,
            "customer_segment": seg,
            "registration_date": reg.isoformat(),
        })
    return pd.DataFrame(rows)


def gen_promotions() -> pd.DataFrame:
    rows = []
    for i in range(10):
        start = START + timedelta(days=random.randint(0, 600))
        end = start + timedelta(days=random.randint(7, 45))
        rows.append({
            "promotion_id": f"PR{200 + i}",
            "promotion_name": f"{random.choice(PROMO_TYPES)} {start.strftime('%b %Y')}",
            "promotion_type": random.choice(PROMO_TYPES),
            "discount_percentage": round(random.choice([5, 10, 15, 20, 25, 30, 40]), 1),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "campaign_channel": random.choice(CHANNELS),
        })
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ sales --
def gen_sales(customers, products, stores, promotions, n: int) -> pd.DataFrame:
    cust_ids = customers["customer_id"].tolist()
    prod = products.set_index("product_id")
    prod_ids = products["product_id"].tolist()
    # popularity weights: cheaper everyday categories sell more units
    pop = {"Grocery": 5, "Apparel": 4, "Footwear": 3, "Beauty": 3,
           "Home & Kitchen": 2, "Toys": 2, "Sports": 2, "Electronics": 1}
    weights = products["category"].map(pop).astype(float).tolist()
    store_ids = stores["store_id"].tolist()
    store_w = np.random.dirichlet(np.ones(len(store_ids)) * 2)  # some stores busier

    promos = promotions.copy()
    promos["start_date"] = pd.to_datetime(promos["start_date"])
    promos["end_date"] = pd.to_datetime(promos["end_date"])

    days = (END - START).days + 1
    day_list = [START + timedelta(days=i) for i in range(days)]
    day_w = np.array([_seasonality(d) for d in day_list])
    day_w = day_w / day_w.sum()

    chosen_days = np.random.choice(day_list, size=n, p=day_w)
    rows = []
    for i, d in enumerate(chosen_days):
        d = pd.Timestamp(d).date()
        pid = random.choices(prod_ids, weights=weights)[0]
        price = float(prod.loc[pid, "selling_price"])
        qty = int(np.random.choice([1, 1, 1, 2, 2, 3, 4, 6], p=[.35, .2, .1, .15, .08, .05, .04, .03]))
        # active promotion?
        active = promos[(promos["start_date"].dt.date <= d) & (promos["end_date"].dt.date >= d)]
        promo_id, discount = None, 0.0
        if len(active) and random.random() < 0.45:
            pr = active.sample(1).iloc[0]
            promo_id = pr["promotion_id"]
            discount = round(qty * price * float(pr["discount_percentage"]) / 100, 2)
        rows.append({
            "transaction_id": f"T{1000000 + i}",
            "transaction_date": d.isoformat(),
            "customer_id": random.choice(cust_ids),
            "product_id": pid,
            "store_id": np.random.choice(store_ids, p=store_w),
            "promotion_id": promo_id,
            "quantity": qty,
            "unit_price": price,
            "discount": discount,
        })
    return pd.DataFrame(rows)


def gen_returns(sales: pd.DataFrame, rate: float = 0.06) -> pd.DataFrame:
    sel = sales.sample(frac=rate, random_state=SEED).reset_index(drop=True)
    rows = []
    for i, r in sel.iterrows():
        ret_qty = int(r["quantity"]) if random.random() < 0.7 else max(1, int(r["quantity"]) - 1)
        ret_date = (pd.Timestamp(r["transaction_date"]) + timedelta(days=random.randint(1, 25))).date()
        rows.append({
            "return_id": f"R{500000 + i}",
            "return_date": ret_date.isoformat(),
            "transaction_id": r["transaction_id"],
            "product_id": r["product_id"],
            "customer_id": r["customer_id"],
            "store_id": r["store_id"],
            "return_quantity": ret_qty,
            "return_reason": random.choice(RETURN_REASONS),
            "refund_amount": round(ret_qty * float(r["unit_price"]) - float(r["discount"]) * ret_qty / int(r["quantity"]), 2),
        })
    return pd.DataFrame(rows)


# ------------------------------------------------------- DQ injection -----
def inject_issues(sales: pd.DataFrame, returns: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Deliberately corrupt ~4-5% of rows so the DQ layer has real findings."""
    manifest: dict[str, list] = {}
    sales = sales.copy()

    def mark(key, idx):
        manifest.setdefault(key, []).extend(sales.loc[idx, "transaction_id"].tolist())

    # 1. null keys
    idx = sales.sample(frac=0.005, random_state=1).index
    sales.loc[idx, "transaction_id"] = None; mark("null_transaction_id", idx)
    idx = sales.sample(frac=0.005, random_state=2).index
    sales.loc[idx, "product_id"] = None; mark("null_product_id", idx)
    # 2. duplicates (1% exact copies, new row numbers, same business key)
    dup = sales.sample(frac=0.01, random_state=3)
    sales = pd.concat([sales, dup], ignore_index=True)
    manifest["duplicate_transaction_id"] = dup["transaction_id"].tolist()
    # 3. orphan FKs
    idx = sales.sample(frac=0.005, random_state=4).index
    sales.loc[idx, "product_id"] = "P99999"; mark("orphan_product_id", idx)
    idx = sales.sample(frac=0.003, random_state=5).index
    sales.loc[idx, "customer_id"] = "C99999"; mark("orphan_customer_id", idx)
    # 4. invalid numerics
    idx = sales.sample(frac=0.004, random_state=6).index
    sales.loc[idx, "quantity"] = np.random.choice([0, -1, -2], len(idx)); mark("invalid_quantity", idx)
    idx = sales.sample(frac=0.002, random_state=7).index
    sales.loc[idx, "discount"] = -round(float(sales.loc[idx, "discount"].fillna(0).abs().max() or 50), 2)
    mark("negative_discount", idx)
    # 5. future dates
    idx = sales.sample(frac=0.002, random_state=8).index
    sales.loc[idx, "transaction_date"] = (date.today() + timedelta(days=random.randint(30, 300))).isoformat()
    mark("future_transaction_date", idx)

    # returns issues
    returns = returns.copy()
    idx = returns.sample(frac=0.02, random_state=9).index
    txn_dates = (sales.dropna(subset=["transaction_id"])
                 .drop_duplicates("transaction_id")
                 .set_index("transaction_id")["transaction_date"])
    returns.loc[idx, "return_date"] = (
        pd.to_datetime(returns.loc[idx, "transaction_id"].map(txn_dates))
        - pd.to_timedelta(np.random.randint(1, 10, len(idx)), unit="D")
    ).dt.date.astype(str)
    manifest["return_before_transaction"] = returns.loc[idx, "return_id"].tolist()
    idx = returns.sample(frac=0.005, random_state=10).index
    returns.loc[idx, "transaction_id"] = "T9999999"
    manifest["orphan_return_transaction"] = returns.loc[idx, "return_id"].tolist()

    manifest["_note"] = ("Answer key for the demo. NOT an input to the pipeline. "
                         "Counts are approximate because sampling rounds overlap.")
    return sales, returns, manifest


# ------------------------------------------------------------------- main --
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-sales", type=int, default=60000)
    ap.add_argument("--n-customers", type=int, default=8000)
    ap.add_argument("--seed", type=int, default=SEED)
    args = ap.parse_args()

    _rng(args.seed)
    out = RAW_DATA_DIR
    out.mkdir(parents=True, exist_ok=True)

    print("Generating masters ...")
    suppliers = gen_suppliers(); suppliers.to_csv(out / "suppliers_mysql_extract.csv", index=False)
    products = gen_products(suppliers); products.to_csv(out / "products_mysql_extract.csv", index=False)
    stores = gen_stores(); stores.to_csv(out / "stores_mysql_extract.csv", index=False)
    customers = gen_customers(args.n_customers); customers.to_csv(out / "customers_oracle_extract.csv", index=False)
    promotions = gen_promotions(); promotions.to_csv(out / "promotions_mysql_extract.csv", index=False)

    print(f"Generating {args.n_sales} sales transactions ...")
    sales = gen_sales(customers, products, stores, promotions, args.n_sales)
    print("Generating returns ...")
    returns = gen_returns(sales)

    print("Injecting data-quality issues (demo answer key) ...")
    sales, returns, manifest = inject_issues(sales, returns)
    sales.to_csv(out / "sales.csv", index=False)
    returns.to_csv(out / "returns.csv", index=False)
    (out / "_injection_manifest.json").write_text(json.dumps(manifest, indent=2))

    n_bad = sum(len(v) for k, v in manifest.items() if not k.startswith("_"))
    print(f"Done. sales={len(sales):,} returns={len(returns):,} injected_issues~{n_bad:,}")
    print(f"Raw data in: {out}")


if __name__ == "__main__":
    main()
