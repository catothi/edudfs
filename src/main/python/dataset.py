"""The sample data set: orders of an online shop in the year 2025.

Deterministic (fixed seed), pure Python + pyarrow. The columns are chosen so that every
Parquet feature of the demo has something to show:

  order_id       int64      1, 2, 3, ...            ascending        -> DELTA_BINARY_PACKED, min/max per row group
  order_ts       timestamp  2025-01-01 .. 12-31     ascending        -> predicate pushdown works out of the box
  country        string     9 values, skewed        random order     -> dictionary + RLE; pushdown only after sorting
  city           string     ~30 values              follows country
  category       string     8 values
  product        string     48 values
  quantity       int32      1..5                    small numbers    -> bit-packing with 3 bits
  unit_price     double     price of the product
  customer_email string     ~40,000 values, 8 % null               -> definition levels, dictionary fallback
  status         string     4 values, 85 % "delivered"             -> long RLE runs
"""

import datetime as dt
import random

import pyarrow as pa

COUNTRIES = {  # country: (weight, cities)
    "DE": (30, ["Berlin", "Hamburg", "Munich", "Ingolstadt", "Cologne"]),
    "FR": (15, ["Paris", "Lyon", "Marseille"]),
    "IT": (12, ["Rome", "Milan", "Naples"]),
    "ES": (10, ["Madrid", "Barcelona", "Valencia"]),
    "NL": (8, ["Amsterdam", "Rotterdam", "Utrecht"]),
    "AT": (8, ["Vienna", "Graz", "Linz"]),
    "PL": (7, ["Warsaw", "Krakow", "Gdansk"]),
    "SE": (5, ["Stockholm", "Gothenburg"]),
    "US": (5, ["New York", "Chicago", "San Francisco", "Austin"]),
}

PRODUCTS = {  # category: [(product, price)]
    "Books": [("Data Lakes for Beginners", 29.9), ("Distributed Systems", 49.0), ("The Parquet Cookbook", 34.5),
              ("SQL Pocket Guide", 14.99), ("Hadoop: The Definitive Guide", 54.9), ("Clean Code", 39.9)],
    "Electronics": [("USB-C Cable", 9.99), ("Wireless Mouse", 24.9), ("Mechanical Keyboard", 89.0),
                    ("27\" Monitor", 249.0), ("Noise Cancelling Headphones", 199.0), ("Power Bank", 39.9)],
    "Fashion": [("T-Shirt", 19.9), ("Hoodie", 49.9), ("Sneakers", 89.9), ("Rain Jacket", 119.0),
                ("Baseball Cap", 17.5), ("Wool Socks", 12.9)],
    "Garden": [("Garden Hose", 29.9), ("Seed Mix", 4.99), ("Pruning Shears", 22.0), ("Plant Pot", 8.5),
               ("Solar Lamp", 15.9), ("Watering Can", 11.9)],
    "Home": [("Coffee Mug", 9.9), ("Desk Lamp", 34.9), ("Throw Pillow", 19.9), ("French Press", 27.5),
             ("Bath Towel", 16.9), ("Scented Candle", 12.5)],
    "Sports": [("Yoga Mat", 24.9), ("Water Bottle", 14.9), ("Running Shoes", 109.0), ("Dumbbell Set", 59.9),
               ("Bike Helmet", 49.9), ("Jump Rope", 7.9)],
    "Toys": [("Building Blocks", 39.9), ("Puzzle 1000", 17.9), ("Board Game", 29.9), ("Plush Bear", 19.9),
             ("RC Car", 59.0), ("Card Game", 9.9)],
    "Beauty": [("Shampoo", 6.9), ("Face Cream", 18.9), ("Lip Balm", 3.5), ("Perfume", 69.0),
               ("Sunscreen", 12.9), ("Hair Dryer", 44.9)],
}

STATUS = [("delivered", 85), ("returned", 8), ("cancelled", 4), ("shipped", 3)]

FIRST = ["anna", "ben", "clara", "david", "emma", "felix", "greta", "hannah", "jonas", "lea", "lukas", "mia",
         "noah", "paul", "sophie", "tim", "lena", "max", "nina", "omar", "sara", "yusuf", "zoe", "elias"]
LAST = ["mueller", "schmidt", "schneider", "fischer", "weber", "meyer", "wagner", "becker", "schulz", "hoffmann",
        "rossi", "martin", "garcia", "dubois", "jansen", "nowak", "smith", "berg", "huber", "bauer"]


def orders(n=200_000, seed=42):
    """The orders table as a pyarrow.Table (in arrival order, i.e. sorted by order_ts)."""
    rnd = random.Random(seed)
    countries = list(COUNTRIES)
    c_weights = [COUNTRIES[c][0] for c in countries]
    categories = list(PRODUCTS)
    statuses, s_weights = zip(*STATUS)
    customers = [f"{rnd.choice(FIRST)}.{rnd.choice(LAST)}{rnd.randint(1, 999)}@example.com" for _ in range(40_000)]

    ts = dt.datetime(2025, 1, 1)
    step = 365 * 24 * 3600 // n  # mean gap between two orders, so that the year is filled
    cols = {k: [] for k in ["order_id", "order_ts", "country", "city", "category", "product", "quantity",
                            "unit_price", "customer_email", "status"]}
    for i in range(1, n + 1):
        ts += dt.timedelta(seconds=rnd.randint(1, 2 * step))
        country = rnd.choices(countries, c_weights)[0]
        category = rnd.choice(categories)
        product, price = rnd.choice(PRODUCTS[category])
        if rnd.random() < 0.1:
            price = round(price * 0.8, 2)  # 20 % discount now and then
        cols["order_id"].append(i)
        cols["order_ts"].append(ts)
        cols["country"].append(country)
        cols["city"].append(rnd.choice(COUNTRIES[country][1]))
        cols["category"].append(category)
        cols["product"].append(product)
        cols["quantity"].append(rnd.choices([1, 2, 3, 4, 5], [60, 20, 10, 6, 4])[0])
        cols["unit_price"].append(price)
        cols["customer_email"].append(None if rnd.random() < 0.08 else rnd.choice(customers))
        cols["status"].append(rnd.choices(statuses, s_weights)[0])

    schema = pa.schema([
        ("order_id", pa.int64()), ("order_ts", pa.timestamp("s")), ("country", pa.string()),
        ("city", pa.string()), ("category", pa.string()), ("product", pa.string()),
        ("quantity", pa.int32()), ("unit_price", pa.float64()), ("customer_email", pa.string()),
        ("status", pa.string()),
    ])
    return pa.table(cols, schema=schema)


if __name__ == "__main__":
    t = orders()
    print(t.schema)
    print(t.slice(0, 5).to_pylist())
