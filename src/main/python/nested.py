"""Nested data in Parquet: lists and structs in one field, and how a page stores them (repetition levels).

    ./parquet nested                    write /shop/orders_nested.parquet, explain the levels, write parquet_nested.html
    ./parquet nested --html out.html    another name for the HTML picture

Parquet stores only the leaves of a nested schema as columns (Dremel encoding): `tags` (a list of strings)
becomes the leaf column `tags.list.element` with one entry per list element. Two small numbers per entry
rebuild the nesting:

  repetition level  0 = this entry starts a new row, k = next element of the k-th list on the path
  definition level  how far down the path the entry is defined: null list, empty list, null element, value

Everything shown here is read back from the pages of the file in EduDFS, not computed from the data.
"""

import argparse
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

import dataset
import demo
import edudfs_fs
import explorer

PATH = "/shop/orders_nested.parquet"
ROWS, ROW_GROUP = 20_000, 10_000
TAGS = ["gift", "express", "b2b", "newsletter", "fragile"]

ITEM = pa.struct([("product", pa.string()), ("qty", pa.int32()), ("unit_price", pa.float64())])
SCHEMA = pa.schema([("order_id", pa.int64()), ("customer", pa.string()), ("tags", pa.list_(pa.string())),
                    ("items", pa.list_(ITEM)), ("packages", pa.list_(pa.list_(pa.string())))])

# The first rows show every case of the levels: a list with several elements, an empty list,
# a null list, a null element inside a list, and a list of lists (packages -> products).
FIRST = [
    {"order_id": 1, "customer": "anna", "tags": ["gift", "express"],
     "items": [{"product": "Wireless Mouse", "qty": 2, "unit_price": 24.9}],
     "packages": [["Wireless Mouse"]]},
    {"order_id": 2, "customer": "ben", "tags": [],
     "items": [{"product": "Desk Lamp", "qty": 1, "unit_price": 34.9},
               {"product": "Coffee Mug", "qty": 2, "unit_price": 9.9}],
     "packages": [["Desk Lamp"], ["Coffee Mug", "Coffee Mug"]]},
    {"order_id": 3, "customer": "clara", "tags": None, "items": [], "packages": []},
    {"order_id": 4, "customer": "david", "tags": ["gift", None], "items": None, "packages": None},
]


def orders():
    """FIRST, then random orders: 1-4 items, 0-3 tags (10 % null), items shipped in 1-2 packages."""
    rnd = random.Random(7)
    products = [p for ps in dataset.PRODUCTS.values() for p in ps]
    rows = list(FIRST)
    for i in range(len(FIRST) + 1, ROWS + 1):
        items = [{"product": p, "qty": rnd.randint(1, 3), "unit_price": price}
                 for p, price in rnd.sample(products, rnd.randint(1, 4))]
        names = [it["product"] for it in items for _ in range(it["qty"])]
        cut = rnd.randint(1, len(names))
        packages = [names[:cut], names[cut:]] if cut < len(names) and rnd.random() < .4 else [names]
        tags = None if rnd.random() < .1 else rnd.sample(TAGS, rnd.randint(0, 3))
        rows.append({"order_id": i, "customer": rnd.choice(dataset.FIRST), "tags": tags, "items": items,
                     "packages": packages})
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def levels_table(info, column, rows=4):
    """The first entries of a leaf column as stored in its first data page: rep, def, value, meaning."""
    c = next(c for c in info["row_groups"][0]["cols"] if c["name"] == column)
    p = next(p for p in c["pages"] if p["type"].startswith("DATA_PAGE"))
    meaning = info["levels"][column]
    reps = p["rep_levels"]["first"] if "rep_levels" in p else [0] * len(p["def_levels"]["first"])
    values = iter((p.get("indices") or {}).get("mapped") or p.get("plain") or [])
    print(f"  leaf column {column}   (max rep {p['max_rep']}, max def {p['max_def']})")
    print(f"  path: {' -> '.join(meaning['path'])}")
    print(f"  {'row':>4}  {'rep':>3}  {'def':>3}  {'value':<20} meaning")
    row = -1
    for r, d in zip(reps, p["def_levels"]["first"]):
        row += r == 0
        if row >= rows:
            break
        v = next(values) if d == p["max_def"] else ""
        text = meaning["def"][d] if r == 0 else f"{meaning['rep'][r]}: {meaning['def'][d]}"
        print(f"  {row if r == 0 else '':>4}  {r:>3}  {d:>3}  {str(v):<20} {text}")
    print()


def main(argv):
    ap = argparse.ArgumentParser(prog="./parquet nested", description=__doc__.split("\n\n")[0])
    ap.add_argument("--html", default="parquet_nested.html", help="file name of the HTML picture")
    a = ap.parse_args(argv)
    try:
        edudfs_fs.status("/")
    except edudfs_fs.WebHdfsError as e:
        sys.exit(f"{e}\nStart EduDFS first: ./hdfs minicluster 3   and in another terminal: ./hdfs httpfs")

    t = orders()
    pq.write_table(t, PATH, filesystem=edudfs_fs.edudfs_filesystem(), row_group_size=ROW_GROUP)
    print(f"\n{'=' * 100}\n Nested data: lists and structs in one field  ->  {PATH}\n{'=' * 100}")
    print("  The first rows (as the application sees them):\n")
    for r in t.slice(0, len(FIRST)).to_pylist():
        print(f"    {r}")
    print()
    info = explorer.analyze(PATH, with_pages=True, samples=6)
    demo.say(f"""The Arrow schema has {len(t.schema)} fields, the Parquet file has {len(info['columns'])} leaf columns -
        one column chunk per leaf in every row group. A struct becomes one leaf per field, a list adds the nodes
        'list' (repeated) and 'element':""")
    for col in info["columns"]:
        lv = info["levels"][col]
        print(f"    {col:<36} max rep {len(lv['rep']) - 1}   max def {len(lv['def']) - 1}")
    print()
    demo.say("""Repetition level: 0 = the entry starts a new row; k = it is the next element of the k-th list on
        the path. Definition level: how many optional/repeated nodes of the path are present - so a null list,
        an empty list and a null element each get their own number, and only an entry with def = max has a value
        in the values section of the page. Read back from the first data page of each leaf:""")
    for col in ("tags.list.element", "items.list.element.product", "packages.list.element.list.element"):
        levels_table(info, col)
    demo.say("""Note: rows 2 and 3 of 'items' take one entry each although they have no item - an empty or null
        list still needs one level entry, otherwise the row would vanish. And 'items.product' and 'items.qty'
        have identical levels: every field of a struct repeats the structure of the list.""")
    explorer.write_html(info, a.html)
    print(f"  HTML picture: {a.html}  ->  open {a.html}  and click a column chunk, e.g. tags or packages.\n")


if __name__ == "__main__":
    main(sys.argv[1:])
