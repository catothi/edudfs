"""Benchmark: the same query on CSV and on differently organized Parquet data in EduDFS, timed.

Used by the web UI (./parquet web, web.py); also usable from the command line:

    ./parquet bench list                       setups and queries
    ./parquet bench run q3 parquet csv         run query q3 on two setups
    ./parquet bench run q3 files_1 files_256 --storage=s3

Setups (all hold the same 200,000 orders):
  format comparison   csv, parquet (time order), shuffled, sorted by country, partitioned by country,
                      partitioned by country and day (~3,300 small files)
  sweep: files        the time-ordered data split into 1 ... 1024 files
  sweep: row groups   one file with 1,000 ... 200,000 rows per row group

A query is timed from opening the dataset (directory listing, footers) to the aggregated result - what
a user waits for. Storage presets add a latency per request and a bandwidth limit (edudfs_fs.STORAGE),
because on localhost requests are almost free and small files would look harmless.
"""

import datetime as dt
import random
import statistics
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import dataset
import demo
import edudfs_fs
from edudfs_fs import IO

STORAGE_PRESETS = {
    "local": {"label": "localhost (no simulation)", "latency_ms": 0, "mb_per_s": 0},
    "hdfs": {"label": "HDFS cluster in a data center: 2 ms per request, 200 MB/s", "latency_ms": 2, "mb_per_s": 200},
    "s3": {"label": "object store (S3) over the internet: 25 ms per request, 50 MB/s", "latency_ms": 25, "mb_per_s": 50},
}

DEC = dt.datetime(2025, 12, 1)
QUERIES = {
    "q1": {"label": "Total revenue 2025",
           "sql": "SELECT sum(unit_price * quantity) FROM orders",
           "columns": ["unit_price", "quantity"], "filter": None, "group": None},
    "q2": {"label": "Revenue per country",
           "sql": "SELECT country, sum(unit_price * quantity) FROM orders GROUP BY country",
           "columns": ["country", "unit_price", "quantity"], "filter": None, "group": "country"},
    "q3": {"label": "Germany in December, per category",
           "sql": "SELECT category, sum(unit_price * quantity) FROM orders\n"
                  " WHERE country = 'DE' AND order_ts >= '2025-12-01' GROUP BY category",
           "columns": ["category", "unit_price", "quantity"],
           "filter": (ds.field("country") == "DE") & (ds.field("order_ts") >= DEC), "group": "category"},
    "q4": {"label": "One order by id (point lookup)",
           "sql": "SELECT * FROM orders WHERE order_id = 123456",
           "columns": None, "filter": ds.field("order_id") == 123456, "group": None},
}

FILE_COUNTS = [1, 4, 16, 64, 256, 1024]
ROW_GROUP_SIZES = [1_000, 5_000, 25_000, 100_000, 200_000]

SETUPS = {
    "csv": {"group": "formats", "label": "CSV", "path": demo.CSV,
            "note": "text, no types, no statistics - every byte is read and parsed"},
    "parquet": {"group": "formats", "label": "Parquet, time order", "path": demo.PLAIN,
                "note": "1 file, 8 row groups, rows in arrival order (= sorted by order_ts)"},
    "shuffled": {"group": "formats", "label": "Parquet, shuffled", "path": "/bench/orders_shuffled.parquet",
                 "note": "same file, rows in random order: min/max of every row group spans everything"},
    "sorted": {"group": "formats", "label": "Parquet, ORDER BY country", "path": demo.SORTED,
               "note": "sorted by country, order_ts: country filters skip row groups"},
    "by_country": {"group": "formats", "label": "Partitioned by country", "path": demo.PARTITIONED,
                   "note": "9 directories country=XX/, one file each"},
    "by_country_day": {"group": "formats", "label": "Partitioned by country + day", "path": "/bench/orders_by_country_day",
                       "note": "~3,300 tiny files - over-partitioned"},
}
for n in FILE_COUNTS:
    SETUPS[f"files_{n}"] = {"group": "files", "label": f"{n} file{'s' if n > 1 else ''}", "x": n,
                            "path": f"/bench/files_{n}", "note": f"time-ordered data split into {n} files"}
for k in ROW_GROUP_SIZES:
    SETUPS[f"rg_{k}"] = {"group": "rowgroups", "label": f"{k:,} rows / row group", "x": k,
                         "path": f"/bench/rowgroups_{k}.parquet",
                         "note": f"1 file, {-(-200_000 // k)} row groups of {k:,} rows"}


# --------------------------------------------------------------------------- creating the setups
def fs():
    return edudfs_fs.edudfs_filesystem()


def build(key):
    """Writes the files of a setup into EduDFS (replaces existing ones)."""
    s = SETUPS[key]
    path = s["path"]
    demo.FS = fs()
    if edudfs_fs.status(path) is not None:
        edudfs_fs.delete(path, recursive=True)
    t = demo.orders()
    if key in ("csv", "parquet", "sorted", "by_country"):
        demo.ensure(path)
    elif key == "shuffled":
        idx = list(range(t.num_rows))
        random.Random(1).shuffle(idx)
        pq.write_table(t.take(pa.array(idx)), path, filesystem=fs(), row_group_size=demo.ROW_GROUP)
    elif key == "by_country_day":
        t2 = t.append_column("day", pc.strftime(t["order_ts"], format="%Y-%m-%d"))
        pq.write_to_dataset(t2, path, filesystem=fs(), partition_cols=["country", "day"],
                            basename_template="part-{i}.parquet")
    elif s["group"] == "files":
        n = s["x"]
        edudfs_fs.mkdirs(path)
        per = -(-t.num_rows // n)
        for i in range(n):
            pq.write_table(t.slice(i * per, per), f"{path}/part-{i:05d}.parquet", filesystem=fs(),
                           row_group_size=demo.ROW_GROUP)
    elif s["group"] == "rowgroups":
        pq.write_table(t, path, filesystem=fs(), row_group_size=s["x"])
    IO.reset()
    return info(key)


def info(key):
    """{exists, files, bytes} of a setup."""
    path = SETUPS[key]["path"]
    if edudfs_fs.status(path) is None:
        return {"exists": False}
    files = all_files(path)
    IO.reset()
    return {"exists": True, "files": len(files), "bytes": sum(n for _, n in files)}


def all_files(path):
    st = edudfs_fs.status(path)
    if st["type"] == "FILE":
        return [(path, st["length"])]
    res = []
    for c in edudfs_fs.listdir(path):
        p = f"{path.rstrip('/')}/{c['pathSuffix']}"
        res += all_files(p) if c["type"] == "DIRECTORY" else [(p, c["length"])]
    return res


# --------------------------------------------------------------------------- running a query
def open_dataset(path):
    if path.endswith(".csv"):
        return ds.dataset(path, filesystem=fs(), format="csv")
    st = edudfs_fs.status(path)
    fmt = ds.ParquetFileFormat(default_fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False))
    return ds.dataset(path, filesystem=fs(), format=fmt, partitioning="hive" if st["type"] == "DIRECTORY" else None)


def execute(path, q):
    """Opens the dataset, reads with projection + filter, aggregates. Returns the result table."""
    d = open_dataset(path)
    t = d.to_table(columns=q["columns"], filter=q["filter"])
    if q["columns"] is None:
        return t.select(["order_id", "order_ts", "country", "product", "quantity", "unit_price"])
    rev = pc.multiply(t["unit_price"], pc.cast(t["quantity"], pa.float64()))
    if q["group"] is None:
        return pa.table({"revenue": [pc.sum(rev).as_py()]})
    g = pa.table({q["group"]: pc.cast(t[q["group"]], pa.string()), "revenue": rev})
    return g.group_by(q["group"]).aggregate([("revenue", "sum")]).sort_by(q["group"])


def run(key, query, storage="local", repeat=3):
    """Times query on setup key. Returns timings and what was read (of the last run)."""
    path, q = SETUPS[key]["path"], QUERIES[query]
    preset = STORAGE_PRESETS[storage]
    edudfs_fs.STORAGE.update(latency_ms=preset["latency_ms"], mb_per_s=preset["mb_per_s"])
    times = []
    try:
        for _ in range(repeat):
            IO.reset()
            t0 = time.perf_counter()
            result = execute(path, q)
            times.append(time.perf_counter() - t0)
        reads, meta = list(IO.reads), IO.meta
    finally:
        edudfs_fs.STORAGE.update(latency_ms=0, mb_per_s=0)
    total = info(key)
    rows = result.slice(0, 12).to_pylist()
    return {"setup": key, "query": query, "storage": storage, "ms": statistics.median(times) * 1000,
            "runs_ms": [x * 1000 for x in times], "bytes_read": sum(n for _, _, n in reads),
            "data_requests": len(reads), "meta_requests": meta, "files_opened": len({p for p, _, _ in reads}),
            "files": total.get("files"), "bytes": total.get("bytes"), "result_rows": result.num_rows,
            "result": [{k: (round(v, 2) if isinstance(v, float) else str(v) if isinstance(v, dt.datetime) else v)
                        for k, v in r.items()} for r in rows]}


def main(argv):
    if not argv or argv[0] == "list":
        for k, s in SETUPS.items():
            print(f"  {k:<16} {s['label']:<32} {s['path']}")
        for k, q in QUERIES.items():
            print(f"  {k}  {q['label']}: {' '.join(q['sql'].split())}")
        return
    if argv[0] == "run":
        storage = next((a.split("=", 1)[1] for a in argv if a.startswith("--storage=")), "local")
        query, keys = argv[1], [a for a in argv[2:] if not a.startswith("--")]
        for k in keys:
            if not info(k)["exists"]:
                print(f"preparing {k} ...", flush=True)
                build(k)
            r = run(k, query, storage)
            print(f"  {k:<16} {r['ms']:8.1f} ms   read {r['bytes_read']:>12,} of {r['bytes']:>12,} B   "
                  f"{r['data_requests']:>5} data + {r['meta_requests']:>5} metadata requests   "
                  f"{r['files_opened']}/{r['files']} files")


if __name__ == "__main__":
    main(sys.argv[1:])
