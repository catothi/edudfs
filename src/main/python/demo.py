"""Guided tour: Parquet on EduDFS, from the file layout to pushdown, sorting, partitioning and encodings.

    ./parquet demo          all steps, one after another (Enter continues)
    ./parquet demo 4        only step 4 (the files of earlier steps are created if missing)
    ./parquet demo list     list the steps

Needs a running EduDFS (./hdfs minicluster 3) and HttpFS (./hdfs httpfs). Watch the HttpFS terminal
while the queries run: every line there is one byte range that the Parquet reader asked for.
"""

import io
import sys
import textwrap

import pyarrow.csv as pacsv
import pyarrow.parquet as pq

import dataset
import edudfs_fs
import explorer
import query
from edudfs_fs import IO

ROOT = "/shop"
CSV = f"{ROOT}/orders.csv"
PLAIN = f"{ROOT}/orders.parquet"                    # arrival order = sorted by order_ts
SORTED = f"{ROOT}/orders_sorted.parquet"            # ORDER BY country, order_ts
PARTITIONED = f"{ROOT}/orders_by_country"           # PARTITIONED BY (country)
TUNED = f"{ROOT}/orders_tuned.parquet"              # sorted + delta encodings + zstd
ROW_GROUP = 25_000

_table = None
FS = None


def orders():
    global _table
    if _table is None:
        _table = dataset.orders()
    return _table


# --------------------------------------------------------------------------- output helpers
def title(n, text):
    print(f"\n{'=' * 100}\n Step {n}: {text}\n{'=' * 100}")


def say(text):
    """Prints a paragraph, re-wrapped (the source lines are indented and f-strings break the dedent)."""
    print(textwrap.fill(" ".join(text.split()), 100) + "\n")


def code(text):
    print(textwrap.indent(textwrap.dedent(text).strip(), "    >>> ") + "\n")


def shell(text):
    print("  Try it in another terminal:")
    print(textwrap.indent(textwrap.dedent(text).strip(), "    $ ") + "\n")


def kb(n):
    return f"{n / 1048576:.2f} MB" if n >= 1048576 else f"{n / 1024:.1f} KB"


def run_query(path, columns=None, where=None, pushdown=True, explain=False):
    sql = f"SELECT {', '.join(columns) if columns else '*'} FROM {path}" + (f" WHERE {where}" if where else "")
    print(f"  {sql}" + ("" if pushdown else "      -- without pushdown: read everything, filter in memory"))
    table, reads, conds = query.run(path, columns, where, pushdown)
    print(f"  -> {table.num_rows:,} rows")
    query.print_io(path, reads, out=lambda s: print("     " + s))
    if explain:
        query.explain(path, conds, out=lambda s: print("     " + s))
    print()
    return sum(n for _, _, n in reads)


# --------------------------------------------------------------------------- files of the steps
def write_parquet(table, path, **kw):
    pq.write_table(table, path, filesystem=FS, row_group_size=ROW_GROUP, **kw)


def ensure(path):
    if edudfs_fs.status(path) is not None:
        return
    if path == CSV:
        buf = io.BytesIO()
        pacsv.write_csv(orders(), buf)
        edudfs_fs.write(CSV, buf.getvalue())
    elif path == PLAIN:
        write_parquet(orders(), PLAIN)
    elif path == SORTED:
        write_parquet(orders().sort_by([("country", "ascending"), ("order_ts", "ascending")]), SORTED,
                      sorting_columns=[pq.SortingColumn(2), pq.SortingColumn(1)])
    elif path == PARTITIONED:
        pq.write_to_dataset(orders(), PARTITIONED, filesystem=FS, partition_cols=["country"],
                            basename_template="part-{i}.parquet", min_rows_per_group=ROW_GROUP,
                            max_rows_per_group=ROW_GROUP)
    elif path == TUNED:
        write_parquet(orders().sort_by([("country", "ascending"), ("order_ts", "ascending")]), TUNED,
                      compression="zstd", use_dictionary=["country", "city", "category", "product", "quantity",
                                                          "unit_price", "customer_email", "status"],
                      column_encoding={"order_id": "DELTA_BINARY_PACKED", "order_ts": "DELTA_BINARY_PACKED"})


# --------------------------------------------------------------------------- the steps
def step1():
    """The data set - and why not CSV?"""
    title(1, step1.__doc__)
    t = orders()
    say(f"""200,000 orders of an online shop in 2025 (src/main/python/dataset.py, always the same thanks to a fixed seed).
        {t.num_columns} columns, in arrival order:""")
    print(textwrap.indent(str(t.schema), "    ") + "\n")
    for row in t.slice(0, 3).to_pylist():
        print("    " + ", ".join(f"{k}={v}" for k, v in row.items()))
    print()
    for p in (CSV, PLAIN):
        edudfs_fs.delete(p) if edudfs_fs.status(p) else None
        ensure(p)
    csv, par = edudfs_fs.status(CSV)["length"], edudfs_fs.status(PLAIN)["length"]
    blk = edudfs_fs.status(PLAIN)["blockSize"]
    say(f"""Stored twice in EduDFS: {CSV} = {kb(csv)} ({-(-csv // blk)} HDFS blocks of {kb(blk)}),
        {PLAIN} = {kb(par)} ({-(-par // blk)} blocks) - {csv / par:.1f}x smaller. CSV has no types (everything
        is text and must be parsed), no compression and no statistics: every query reads every byte.
        Parquet stores the column type in the schema, encodes and compresses column by column and keeps
        min/max statistics in the footer.""")
    code("""
        table = dataset.orders()
        pq.write_table(table, "/shop/orders.parquet", filesystem=edudfs_filesystem(), row_group_size=25_000)""")
    shell(f"""
        ./hdfs dfs -ls -R {ROOT}
        ./hdfs dfs -metasave""")


def step2():
    """Inside the file: magic, row groups, column chunks, pages, footer"""
    title(2, step2.__doc__)
    ensure(PLAIN)
    info = explorer.analyze(PLAIN, samples=6)
    say(f"""A Parquet file is 'PAR1' + row groups + footer + footer length + 'PAR1'. Each row group holds
        a horizontal slice of the rows ({ROW_GROUP:,} here); inside it every column has its own column chunk,
        and every chunk consists of pages (a dictionary page, then data pages). The footer (FileMetaData)
        describes all of it: schema, and for every column chunk its offset, size, encodings and min/max.""")
    explorer.print_layout(info, pages=False, chunks_of=1)
    print()
    say("""The same metadata from Python, as on the slides (pq.ParquetFile -> metadata -> row_group(0) ->
        column(i).statistics). A reader gets ALL of this from the last few KB of the file:""")
    code("""
        pf = pq.ParquetFile(fs.open_input_file("/shop/orders.parquet"))
        pf.metadata.row_group(0).column(1).statistics""")
    md = pq.ParquetFile(FS.open_input_file(PLAIN)).metadata
    for i in range(md.num_columns):
        st = md.row_group(0).column(i).statistics
        print(f"    {md.schema.column(i).name:<15} min={str(st.min):<26.26} max={str(st.max):<26.26} nulls={st.null_count}")
    print()
    explorer.write_html(info, "parquet.html")
    say("""Note the HDFS blocks: a row group does not respect block boundaries - several row groups share
        a block, some are split between two. That is why the rule of thumb says: row group size = HDFS
        block size (or a fraction of it), so one row group can be read from one DataNode.
        An interactive picture of the file was written to parquet.html.""")
    shell(f"""
        open parquet.html                      (Linux: xdg-open parquet.html)
        ./parquet view --pages -f {PLAIN}
        ./parquet view -m -f {PLAIN}""")


def step3():
    """Projection pushdown: read only the columns you need"""
    title(3, step3.__doc__)
    ensure(CSV)
    ensure(PLAIN)
    say("""Revenue question: SELECT unit_price ... A CSV reader must read every line and cut out one field.
        Parquet stores each column chunk contiguously, so the reader asks HttpFS for exactly the byte
        ranges of the unit_price chunks (offsets from the footer).""")
    a = run_query(CSV, ["unit_price"])
    b = run_query(PLAIN, ["unit_price"], pushdown=False)
    c = run_query(PLAIN, ["unit_price"])
    say(f"""CSV {kb(a)} -> Parquet without pushdown {kb(b)} -> Parquet with projection pushdown {kb(c)}.
        The first request of every Parquet read is the last 64 KB (footer). Look at the HttpFS terminal:
        one OPEN line per column chunk range, each with the HDFS block it falls into.""")
    code("""
        dataset = ds.dataset("/shop/orders.parquet", filesystem=edudfs_filesystem(), format="parquet")
        dataset.to_table(columns=["unit_price"])""")


def step4():
    """Predicate pushdown: skip row groups using the min/max statistics"""
    title(4, step4.__doc__)
    ensure(PLAIN)
    say("""A filter is checked against the min/max of every row group BEFORE any data is read. If the
        range cannot contain a match, the whole row group is skipped. The orders arrive in time order, so
        each row group covers about 6 weeks - a filter on order_ts can skip almost everything:""")
    run_query(PLAIN, ["order_id", "unit_price"], "order_ts >= '2025-12-01'", explain=True)
    say("""Now a filter on country. Every row group contains orders from all countries, min = 'AT' and
        max = 'US' everywhere - 'DE' lies in every range, nothing can be skipped (only the projection helps):""")
    run_query(PLAIN, ["order_id", "unit_price"], "country = 'DE'", explain=True)
    say("""And a value that does not exist at all: status = 'refunded'. 'cancelled' <= 'refunded' <= 'shipped',
        so min/max cannot prove that it is missing - all row groups are read and 0 rows come back. This is
        what Bloom filters are for (they answer 'definitely not here'). They are off by default
        and must be enabled per column when writing (Spark: parquet.bloom.filter.enabled#column); Spark then uses
        them automatically when reading. pyarrow can write them (bloom_filter_options=...) but does not skip with them.""")
    run_query(PLAIN, ["order_id"], "status = 'refunded'")
    code("""
        dataset.to_table(columns=["order_id", "unit_price"], filter=ds.field("order_ts") >= datetime(2025, 12, 1))""")


def step5():
    """ORDER BY before writing: make the statistics selective"""
    title(5, step5.__doc__)
    ensure(PLAIN)
    ensure(SORTED)
    say("""Same data, sorted by country (then order_ts) before writing. Now every row group holds one or
        two countries, min/max become narrow, and the filter on country can skip row groups:""")
    run_query(SORTED, ["order_id", "unit_price"], "country = 'DE'", explain=True)
    p, s = (pq.ParquetFile(FS.open_input_file(f)).metadata for f in (PLAIN, SORTED))
    IO.reset()

    def chunk(md, name):
        i = [md.schema.column(k).name for k in range(md.num_columns)].index(name)
        return sum(md.row_group(r).column(i).total_compressed_size for r in range(md.num_row_groups))
    say("""Sorting also helps the encoding: equal values now stand next to each other, the RLE runs of the
        dictionary indices become long. Size of the column on disk, unsorted vs. sorted:""")
    for name in ("country", "city", "status"):
        print(f"    {name:<10} {chunk(p, name):>10,} B  ->  {chunk(s, name):>8,} B")
    print()
    say("""The price: only ONE sort order per file. A filter on order_ts now skips less than before (try it).
        sorting_columns in the footer records the order, so engines can use it.""")
    code("""
        sorted_table = table.sort_by([("country", "ascending"), ("order_ts", "ascending")])
        pq.write_table(sorted_table, "/shop/orders_sorted.parquet", filesystem=fs, row_group_size=25_000)""")
    shell(f"""
        ./parquet query {SORTED} --where "order_ts >= '2025-12-01'" --explain
        ./parquet view --html parquet_sorted.html -f {SORTED}""")


def step6():
    """Column partitioning: one directory per value"""
    title(6, step6.__doc__)
    if edudfs_fs.status(PARTITIONED):
        edudfs_fs.delete(PARTITIONED, recursive=True)
    ensure(PARTITIONED)
    say("""pq.write_to_dataset(..., partition_cols=["country"]) writes one directory per country
        (Hive style: country=DE/). The column itself is no longer stored in the files - its value is in the
        path. A filter on country is decided from the directory names alone: the other files are not even
        opened (partition pruning).""")
    files = query.all_files(PARTITIONED)
    for p, size in files:
        print(f"    {p:<50} {size:>10,} B")
    print()
    run_query(PARTITIONED, ["order_id", "unit_price"], "country = 'DE'")
    say("""Partition pruning and predicate pushdown combine - first the directory, then the row groups
        inside (order_ts is still in time order within each partition):""")
    run_query(PARTITIONED, ["order_id", "unit_price"], "country = 'DE' and order_ts >= '2025-12-01'", explain=True)
    small = sum(1 for _, s in files if s < edudfs_fs.status(PLAIN)["blockSize"])
    say(f"""Watch the sizes: {small} of {len(files)} files are smaller than one HDFS block. With a real 128 MB
        block size and partitioning by day or by customer this becomes the 'small files problem' - every
        file costs a NameNode object, a footer read and a task. Partition only by columns with few values
        that (almost) every query filters on; compact small files.""")
    code("""
        pq.write_to_dataset(table, "/shop/orders_by_country", filesystem=fs, partition_cols=["country"])
        ds.dataset("/shop/orders_by_country", filesystem=fs, partitioning="hive").to_table(filter=ds.field("country") == "DE")""")
    shell(f"./hdfs dfs -ls -R {PARTITIONED}")


def step7():
    """Encodings and compression"""
    title(7, step7.__doc__)
    t = orders()
    say("""Before compression, Parquet encodes the values of each page. Here every column is written on its
        own with different encodings and codecs (in memory) - size of the column on disk:""")

    def size(col, comp, **kw):
        buf = io.BytesIO()
        pq.write_table(t.select([col]), buf, row_group_size=ROW_GROUP, compression=comp, **kw)
        md = pq.ParquetFile(io.BytesIO(buf.getvalue())).metadata
        return sum(md.row_group(r).column(0).total_compressed_size for r in range(md.num_row_groups))
    rows = [("order_id", "1, 2, 3, ...", "DELTA_BINARY_PACKED"),
            ("order_ts", "ascending timestamps", "DELTA_BINARY_PACKED"),
            ("country", "9 distinct values", None),
            ("product", "48 distinct strings", "DELTA_BYTE_ARRAY"),
            ("customer_email", "~40,000 distinct strings", "DELTA_LENGTH_BYTE_ARRAY"),
            ("unit_price", "75 distinct doubles", "BYTE_STREAM_SPLIT")]
    print(f"    {'column':<15} {'values':<26} {'codec':<7} {'PLAIN':>10} {'DICTIONARY':>11}   other encoding")
    for col, desc, enc in rows:
        for comp in ("none", "snappy", "zstd"):
            plain = size(col, comp, use_dictionary=False)
            dic = size(col, comp)
            other = f"{size(col, comp, use_dictionary=False, column_encoding={col: enc}):>10,}  {enc}" if enc else ""
            print(f"    {col if comp == 'none' else '':<15} {desc if comp == 'none' else '':<26} {comp:<7} "
                  f"{plain:>10,} {dic:>11,}   {other}")
    print()
    say("""What to see: DICTIONARY (the default) wins for few distinct values - the page then only holds small
        indices (RLE + bit-packing: 9 countries need 4 bits). DELTA_BINARY_PACKED wins for ascending numbers
        and timestamps (order_id: almost nothing left - all deltas are 1). For high-cardinality strings no
        encoding helps much; the codec (snappy, zstd) does the rest. BYTE_STREAM_SPLIT is made for noisy
        floating point data (sensor readings); with 75 distinct prices the dictionary is better.""")
    ensure(TUNED)
    tuned, plain = edudfs_fs.status(TUNED)["length"], edudfs_fs.status(PLAIN)["length"]
    say(f"""Everything together - sorted by country, DELTA_BINARY_PACKED for order_id/order_ts, zstd:
        {TUNED} = {kb(tuned)} instead of {kb(plain)} ({plain / tuned:.1f}x smaller, without losing a single value).""")
    code("""
        pq.write_table(sorted_table, "/shop/orders_tuned.parquet", filesystem=fs, compression="zstd",
                       use_dictionary=[... all but order_id, order_ts ...],
                       column_encoding={"order_id": "DELTA_BINARY_PACKED", "order_ts": "DELTA_BINARY_PACKED"})""")
    shell(f"./parquet view --pages -f {TUNED}")


def step8():
    """Best practices - what the demo has shown"""
    title(8, step8.__doc__)
    for line in ["1. Columnar + footer statistics: read only the columns (projection) and row groups (predicate) you need.",
                 "2. Sort by the column you filter on most - otherwise min/max covers everything and nothing is skipped.",
                 "3. Partition by a low-cardinality column that almost every query filters on; avoid many small files.",
                 "4. Row group size ~ HDFS block size (or a fraction), files of 128 MB to 1 GB; compact small files.",
                 "5. Let the encodings work: dictionary for few values, delta for sorted numbers/timestamps, then a codec.",
                 "6. min/max cannot prove that a value is absent inside the range - Bloom filters can (off by default, enable per column)."]:
        print("  " + line)
    print()
    shell("./parquet query /shop/orders_tuned.parquet --columns unit_price --where \"country = 'DE'\" --explain")


STEPS = [step1, step2, step3, step4, step5, step6, step7, step8]


def main(argv):
    global FS
    if argv and argv[0] == "list":
        for i, s in enumerate(STEPS, 1):
            print(f"  {i}  {s.__doc__}")
        return
    try:
        edudfs_fs.status("/")
    except edudfs_fs.WebHdfsError as e:
        sys.exit(f"{e}\nStart EduDFS first: ./hdfs minicluster 3   and in another terminal: ./hdfs httpfs")
    FS = edudfs_fs.edudfs_filesystem()
    if argv:
        STEPS[int(argv[0]) - 1]()
        return
    for i, s in enumerate(STEPS):
        s()
        if i < len(STEPS) - 1 and sys.stdin.isatty():
            input("  [Enter] next step ")


if __name__ == "__main__":
    main(sys.argv[1:])
