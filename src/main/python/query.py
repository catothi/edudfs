"""Query Parquet files in EduDFS with pyarrow - and see what was actually read.

    ./parquet query /shop/orders.parquet --columns unit_price --where "order_ts >= '2025-12-01'"
    ./parquet query /shop/orders.parquet --where "country = 'DE'" --explain
    ./parquet query /shop/orders.parquet --columns unit_price --no-pushdown
    ./parquet query /shop/orders_by_country --where "country = 'DE'"      a partitioned directory

pyarrow.dataset does both kinds of pushdown itself:
  projection pushdown  only the column chunks of the requested columns are read
  predicate pushdown   row groups whose min/max statistics rule out the filter are skipped,
                       partition directories (country=DE/) that do not match are not even opened
The I/O report counts every byte range that went over HttpFS (see edudfs_fs.IO).
"""

import argparse
import datetime as dt
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import edudfs_fs
from edudfs_fs import IO

FOOTER_TAIL = 64 * 1024   # pyarrow reads the last 64 KB in one go, hoping the footer fits


# --------------------------------------------------------------------------- the WHERE clause
def parse_where(where, schema):
    """"country = 'DE' and quantity >= 4" -> [(column, op, value)], value converted to the column type."""
    conds = []
    for part in re.split(r"\s+and\s+", where.strip(), flags=re.I) if where and where.strip() else []:
        m = re.fullmatch(r"(\w+)\s*(==|=|!=|<=|>=|<|>)\s*(.+)", part.strip())
        if not m or m.group(1) not in schema.names:
            raise SystemExit(f"cannot read '{part.strip()}' - use: column op value (columns: {', '.join(schema.names)})")
        name, op, raw = m.group(1), m.group(2).replace("==", "="), m.group(3).strip().strip("'\"")
        t = schema.field(name).type
        if pa.types.is_integer(t):
            v = int(raw)
        elif pa.types.is_floating(t):
            v = float(raw)
        elif pa.types.is_timestamp(t) or pa.types.is_date(t):
            v = dt.datetime.fromisoformat(raw)
        else:
            v = raw
        conds.append((name, op, v))
    return conds


def to_expression(conds):
    ops = {"=": lambda f, v: f == v, "!=": lambda f, v: f != v, "<": lambda f, v: f < v,
           "<=": lambda f, v: f <= v, ">": lambda f, v: f > v, ">=": lambda f, v: f >= v}
    expr = None
    for name, op, v in conds:
        e = ops[op](ds.field(name), v)
        expr = e if expr is None else expr & e
    return expr


def may_match(stats, op, v):
    """The test a reader does per row group: can min..max contain a value that satisfies 'op v'?"""
    if stats is None or not stats.has_min_max:
        return True
    lo, hi = stats.min, stats.max
    if isinstance(v, dt.datetime) and not isinstance(lo, dt.datetime):
        return True
    return {"=": lo <= v <= hi, "!=": not (lo == v == hi), "<": lo < v, "<=": lo <= v,
            ">": hi > v, ">=": hi >= v}[op]


# --------------------------------------------------------------------------- running a query
def open_dataset(path):
    fs = edudfs_fs.edudfs_filesystem()
    st = edudfs_fs.status(path)
    if st is None:
        raise SystemExit(f"{path} does not exist in EduDFS")
    # pre_buffer=False: one request per column chunk. pyarrow's default (True) merges neighboring
    # ranges into fewer, larger requests - faster on object stores, but harder to read in a demo.
    fmt = "csv" if path.endswith(".csv") else ds.ParquetFileFormat(
        default_fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False))
    return ds.dataset(path, filesystem=fs, format=fmt,
                      partitioning="hive" if st["type"] == "DIRECTORY" else None)


def run(path, columns=None, where=None, pushdown=True):
    """Runs the query; returns (result table, list of read ranges). IO is reset first."""
    dataset = open_dataset(path)
    conds = parse_where(where, dataset.schema)
    expr = to_expression(conds)
    IO.reset()
    if pushdown:
        table = dataset.to_table(columns=columns, filter=expr)
    else:
        # what a reader without pushdown does: read everything, then filter and project in memory
        table = dataset.to_table()
        if expr is not None:
            table = table.filter(expr)
        if columns:
            table = table.select(columns)
    return table, list(IO.reads), conds


def file_report(path, ranges):
    """What the ranges of one file were: footer, which row groups / column chunks, which HDFS blocks."""
    IO.reset()
    st = edudfs_fs.status(path)
    size = st["length"]
    md = pq.ParquetFile(edudfs_fs.edudfs_filesystem().open_input_file(path)).metadata
    IO.reset()

    def touched(o, n):
        return any(ro < o + n and ro + rn > o for ro, rn in ranges
                   if not (ro + rn == size and rn <= FOOTER_TAIL))      # ignore the footer read
    rgs, chunks = set(), 0
    for r in range(md.num_row_groups):
        rg = md.row_group(r)
        for c in range(rg.num_columns):
            cc = rg.column(c)
            start = cc.dictionary_page_offset if cc.has_dictionary_page else cc.data_page_offset
            if touched(start, cc.total_compressed_size):
                rgs.add(r)
                chunks += 1
    blocks = edudfs_fs.block_locations(path)
    blocks_read = [b for b in blocks if any(ro < b["offset"] + b["length"] and ro + rn > b["offset"]
                                            for ro, rn in ranges)]
    return {"size": size, "read": sum(n for _, n in ranges), "requests": len(ranges), "row_groups": md.num_row_groups,
            "rgs_read": sorted(rgs), "chunks": md.num_row_groups * md.num_columns, "chunks_read": chunks,
            "blocks": blocks, "blocks_read": blocks_read, "metadata": md}


def all_files(path):
    st = edudfs_fs.status(path)
    if st["type"] == "FILE":
        return [("/" + path.lstrip("/"), st["length"])]
    res = []
    for child in edudfs_fs.listdir(path):
        p = f"{path.rstrip('/')}/{child['pathSuffix']}"
        res += all_files(p) if child["type"] == "DIRECTORY" else [(p, child["length"])]
    return res


def print_io(path, reads, out=print):
    files = all_files(path)
    total = sum(s for _, s in files)
    read = sum(n for _, _, n in reads)
    out(f"I/O over HttpFS: {len(reads)} read requests, {read:,} of {total:,} bytes ({read / total * 100:.1f} %)"
        f" from {len({p for p, _, _ in reads})} of {len(files)} file(s)")
    for p, size in files:
        ranges = [(o, n) for fp, o, n in reads if fp == p]
        if not ranges:
            if len(files) > 1:
                out(f"  {p:<48} {size:>10,} B   not opened")
            continue
        if not p.endswith(".parquet"):
            read_p = sum(n for _, n in ranges)
            out(f"  {p:<48} {size:>10,} B   read {read_p:>9,} B in {len(ranges):>2} requests | no footer, no statistics:"
                f" every byte is read and parsed")
            continue
        r = file_report(p, ranges)
        out(f"  {p:<48} {size:>10,} B   read {r['read']:>9,} B in {r['requests']:>2} requests | "
            f"row groups {len(r['rgs_read'])}/{r['row_groups']} | column chunks {r['chunks_read']}/{r['chunks']} | "
            f"HDFS blocks {len(r['blocks_read'])}/{len(r['blocks'])}")


def partition_values(path):
    """/shop/orders_by_country/country=DE/part-0.parquet -> {'country': 'DE'} (hive partitioning)."""
    return dict(seg.split("=", 1) for seg in path.split("/") if "=" in seg)


def explain(path, conds, out=print):
    """Shows the decisions the reader takes before reading data: partition directories, then row group statistics."""
    for p, _ in all_files(path):
        parts = partition_values(p)
        pruned = [f"{name}={parts[name]}" for name, op, v in conds
                  if name in parts and not may_match_value(parts[name], op, str(v))]
        if pruned:
            out(f"{p}: SKIP the whole file - directory {', '.join(pruned)} does not match (partition pruning)")
            continue
        md = pq.ParquetFile(edudfs_fs.edudfs_filesystem().open_input_file(p)).metadata
        names = [md.schema.column(i).name for i in range(md.num_columns)]
        stored = [(n, op, v) for n, op, v in conds if n in names]
        if not stored:
            out(f"{p}: read all {md.num_row_groups} row groups - no condition on a column stored in the file")
            continue
        out(f"{p}: footer statistics of {md.num_row_groups} row groups")
        for r in range(md.num_row_groups):
            rg = md.row_group(r)
            checks, keep = [], True
            for name, op, v in stored:
                stats = rg.column(names.index(name)).statistics
                ok = may_match(stats, op, v)
                keep &= ok
                checks.append(f"{name} in [{stats.min} .. {stats.max}] {op} {v}? {'maybe' if ok else 'NO'}")
            out(f"  row group {r}: {'read' if keep else 'SKIP'}   " + "; ".join(checks))
    IO.reset()


def may_match_value(value, op, v):
    return {"=": value == v, "!=": value != v, "<": value < v, "<=": value <= v, ">": value > v, ">=": value >= v}[op]


def show(table, n=5, out=print):
    out(f"{table.num_rows:,} rows, columns: {', '.join(table.column_names)}")
    for row in table.slice(0, n).to_pylist():
        out("  " + ", ".join(f"{k}={v}" for k, v in row.items()))
    if table.num_rows > n:
        out("  ...")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="./parquet query", description="Query Parquet in EduDFS with pushdown")
    ap.add_argument("path", help="file or (partitioned) directory in EduDFS")
    ap.add_argument("--columns", help="comma-separated columns (default: all)")
    ap.add_argument("--where", help="filter, e.g. \"country = 'DE' and quantity >= 4\"")
    ap.add_argument("--no-pushdown", action="store_true", help="read everything, filter afterwards in memory")
    ap.add_argument("--explain", action="store_true", help="show the row group decisions from the footer statistics")
    ap.add_argument("--sum", metavar="COLUMN", help="also print the sum of a column (e.g. unit_price)")
    a = ap.parse_args(argv)
    columns = a.columns.split(",") if a.columns else None
    table, reads, conds = run(a.path, columns, a.where, pushdown=not a.no_pushdown)
    show(table)
    if a.sum:
        print(f"sum({a.sum}) = {pc.sum(table[a.sum]).as_py():,.2f}")
    print()
    print_io(a.path, reads)
    if a.explain:
        print()
        explain(a.path, conds)


if __name__ == "__main__":
    main()
