# Parquet on EduDFS – Hands-on

> Apache Parquet files stored in EduDFS, read with Python (pyarrow) – and watched byte by byte: which parts of a
> file a query really reads, why sorting and partitioning change that, and what the encodings do.
>
> Apache Parquet and Apache Arrow are projects of the Apache Software Foundation; this hands-on only *uses*
> pyarrow. See the [README](../README.md#license-and-trademarks) for license and trademark notices.

What you will see, step by step:

1. a sample data set (200,000 orders of an online shop) as **CSV vs. Parquet** in EduDFS,
2. **inside a Parquet file**: magic, row groups, column chunks, pages (page header, repetition/definition levels,
   values), footer – in the terminal and as an interactive HTML picture,
3. **projection pushdown** – only the column chunks of the selected columns are read,
4. **predicate pushdown** – row groups are skipped using the min/max statistics in the footer,
5. **ORDER BY** before writing – makes the statistics selective (and the file smaller),
6. **column partitioning** – one directory per value, files that do not match are not even opened,
7. **encodings and compression** – dictionary, RLE/bit-packing, delta, byte stream split; snappy vs. zstd,
8. best practices (row group size vs. HDFS block size, small files, Bloom filters).

Every number in this document comes from a real run (`./parquet demo`, pyarrow 26, EduDFS with 1 MB blocks).

## Contents

1. [Setup](#1-setup)
2. [How Python reads from EduDFS](#2-how-python-reads-from-edudfs) – and why no Spark is needed
3. [The data set](#3-the-data-set)
4. [The Parquet file format](#4-the-parquet-file-format)
5. [Tools: `./parquet view`, the HTML explorer, `./parquet query`](#5-tools)
6. [Hands-on, step by step](#6-hands-on-step-by-step)
6a. [Timing it: the benchmark web UI](#6a-timing-it-the-benchmark-web-ui) – CSV vs. Parquet layouts, sweet spots
6b. [Nested data: repetition and definition levels](#6b-nested-data-repetition-and-definition-levels) – lists and structs in one field
7. [Encodings and codecs (reference)](#7-encodings-and-codecs-reference)
8. [Best practices](#8-best-practices)
9. [Exercises](#9-exercises)
10. [The same with Spark (optional)](#10-the-same-with-spark-optional)
11. [Simplifications](#11-simplifications)
12. [Troubleshooting](#12-troubleshooting)
13. [References](#13-references)

---

## 1. Setup

Requirements: Java 21+, Python 3.9+ with `venv`. Once:

```bash
./hdfs build
./parquet setup
```

`./parquet setup` creates `.venv/` in the project directory and installs `pyarrow` (the only Python dependency,
`src/main/python/requirements.txt`). Then **three terminals**:

| Terminal | Command | Role |
|---|---|---|
| 1 | `./hdfs minicluster 3` | EduDFS: NameNode + 3 DataNodes, block size 1 MB (`conf/hdfs-site.xml`) |
| 2 | `./hdfs httpfs` | REST gateway on port 14000 – **watch this one**: every read request appears here |
| 3 | `./parquet demo` | the guided tour (Enter = next step) |

```bash
./parquet demo list
./parquet demo 4
```

`./parquet demo <n>` runs a single step; files of earlier steps are created if missing. Everything is written
under `/shop` in EduDFS – `./hdfs dfs -ls -R /shop` shows it, `./hdfs dfs -rm -r /shop` removes it.

The launcher `./parquet`:

| Command | starts |
|---|---|
| `./parquet setup` | creates `.venv/`, installs pyarrow |
| `./parquet demo [n\|list]` | `src/main/python/demo.py` – guided tour |
| `./parquet view [--pages] [-m] [--html out.html] -f <path>` | `explorer.py` – look inside a file (`file:<local path>` for a local one) |
| `./parquet nested [--html out.html]` | `nested.py` – nested example, see [6b](#6b-nested-data-repetition-and-definition-levels) |
| `./parquet query <path> [--columns a,b] [--where …] [--explain] [--no-pushdown] [--sum col]` | `query.py` – query with I/O report |
| `./parquet web [port]` | `web.py` – benchmark web UI on <http://localhost:8050/>, see [6a](#6a-timing-it-the-benchmark-web-ui) |
| `./parquet bench list` / `./parquet bench run <query> <setup>… [--storage=local\|hdfs\|s3]` | `bench.py` – the same benchmark on the command line |
| `./parquet python [script]` | Python of the venv with the demo modules on the path |

## 2. How Python reads from EduDFS

```
 ./parquet query / demo                         ./hdfs httpfs                    ./hdfs minicluster
┌──────────────────────────┐   HTTP GET      ┌──────────────────────┐  RMI/TCP  ┌───────────────────┐
│ pyarrow.dataset          │ op=OPEN&offset  │ HttpFSServer         │ ────────▶ │ NameNode          │
│   │ Parquet reader       │ =o&length=n     │  DFSClient           │           │ (block locations) │
│   ▼                      │ ──────────────▶ │  DFSInputStream      │ ────────▶ │ DataNodes         │
│ PyFileSystem             │ ◀────────────── │   .readFully(o, n)   │  (pread)  │ (block files)     │
│  (edudfs_fs.py)          │  exactly n B    │  logs blocks touched │           │                   │
│  IO: list of all ranges  │                 └──────────────────────┘           └───────────────────┘
└──────────────────────────┘
```

* pyarrow talks to a real HDFS through `libhdfs` (JNI + Hadoop jars). EduDFS has its own protocol, so
  `src/main/python/edudfs_fs.py` implements a **pyarrow file system** (`pyarrow.fs.FileSystemHandler`) on top of the
  WebHDFS REST API that `./hdfs httpfs` provides ([DOCUMENTATION.md 11.6](DOCUMENTATION.md#116-httpfs-webhdfs-rest-gateway)).
* Every `read()` of the Parquet reader becomes **exactly one** `op=OPEN&offset=…&length=…` request – no read-ahead,
  no cache. `edudfs_fs.IO` records every range; that is the I/O report of `./parquet query`.
* The HttpFS terminal shows the same requests from the file system's side, with the HDFS blocks they fall into:

  ```
  [HttpFS] OPEN /shop/orders.parquet bytes [5,088,798 .. 5,154,334) = 65,536 B  <- blk_1073741867 (65,536 B)
  [HttpFS] OPEN /shop/orders.parquet bytes [408,893 .. 431,374) = 22,481 B  <- blk_1073741863 (22,481 B)
  [HttpFS] OPEN /shop/orders.parquet bytes [1,052,999 .. 1,075,483) = 22,484 B  <- blk_1073741864 (22,484 B)
  ```

  First the last 64 KB (footer), then one request per `unit_price` column chunk.
* `query.py` switches off pyarrow's `pre_buffer` (merging of neighboring ranges into fewer, bigger requests), so that
  one request = one column chunk. Real engines keep it on: on object stores (S3) and HDFS, fewer requests are faster.

**Is PySpark needed to see pushdown?** No. Pushdown is done by the **Parquet reader**, not by the engine around it:
pyarrow (like Spark, Trino, DuckDB, Hive) reads the footer, prunes row groups by statistics and reads only the
needed column chunks. Spark shows pushdown as `PushedFilters: [...]` in `explain()` and as "bytes read" in the UI –
here you see the actual byte ranges, which is more direct. Spark is optional, see [10](#10-the-same-with-spark-optional).

## 3. The data set

`src/main/python/dataset.py` generates 200,000 orders of the year 2025 (fixed seed, always the same data). The
columns are chosen so that every feature has something to show:

| Column | Type | Values | Shows |
|---|---|---|---|
| `order_id` | int64 | 1, 2, 3, … ascending | `DELTA_BINARY_PACKED` (all deltas = 1) |
| `order_ts` | timestamp | 2025-01-01 … 12-31, ascending | predicate pushdown works without sorting |
| `country` | string | 9 values, skewed (DE 30 %) | dictionary + RLE; pushdown only after sorting; partition key |
| `city` | string | ~30 values, depends on country | |
| `category`, `product` | string | 8 / 48 values | dictionary |
| `quantity` | int32 | 1 … 5 | bit-packing with 3 bits |
| `unit_price` | double | 75 distinct prices | projection pushdown ("revenue") |
| `customer_email` | string | ~40,000 values, **8 % null** | definition levels; high cardinality |
| `status` | string | 4 values, 85 % `delivered` | long RLE runs; `'refunded'` does not exist (Bloom filter case) |

Size in EduDFS: CSV **21.11 MB** (22 blocks) vs. Parquet **4.92 MB** (5 blocks), the same 200,000 rows.

## 4. The Parquet file format

```
offset 0   ┌──────────────────────────────────────────────────────────────┐
           │ "PAR1"  (magic, 4 bytes)                                     │
4          ├─ row group 0  (25,000 rows) ─────────────────────────────────┤
           │  column chunk order_id   ┌ page: dictionary page            │
           │                          ├ page: data page ┌ page header     │  (Thrift compact)
           │                          │                 ├ repetition levels│  (nested data only)
           │                          │                 ├ definition levels│  (which values are null)
           │                          │                 └ values           │  (encoded, then compressed)
           │                          └ page: data page …                 │
           │  column chunk order_ts …                                     │
           │  … one chunk per column, contiguous …                         │
644,118    ├─ row group 1 … row group 7 ──────────────────────────────────┤
5,144,358  ├─ footer: FileMetaData (Thrift compact) ──────────────────────┤
           │  version, schema, key/value metadata (ARROW:schema)          │
           │  per row group: num_rows, total size, and per column chunk:  │
           │    type, path, encodings, codec, num_values,                 │
           │    offset of dictionary/first data page, (un)compressed size,│
           │    statistics: min, max, null_count                          │
5,154,326  ├─ footer length (4 bytes, little endian) = 9,968               │
5,154,330  ├─ "PAR1"                                                       │
           └──────────────────────────────────────────────────────────────┘
```

* **Row group** – a horizontal slice of the rows (here 25,000; `row_group_size` when writing). Unit of
  predicate pushdown and of parallelism (one task per row group).
* **Column chunk** – the values of *one* column within a row group, stored contiguously. Unit of projection
  pushdown: one column = one byte range per row group.
* **Page** – unit of encoding and compression (default ~1 MB uncompressed, often several per chunk). With dictionary
  encoding, a chunk starts with a **dictionary page** (the distinct values, PLAIN), followed by **data pages** that
  only contain indices into the dictionary.
* **Data page v1** = page header + `[repetition levels][definition levels][values]`, compressed together. Flat
  columns have no repetition levels (max level 0); every column written by pyarrow is `optional`, so every data page
  has definition levels (1 = value present, 0 = null) – RLE-encoded, so a column without nulls costs only a few bytes.
* **Footer** – everything a reader needs to plan: it reads the last 8 bytes (length + magic), then the footer,
  then **only** the column chunks it needs. That is why Parquet files are written once and never modified in place:
  the footer is written last.

A real page from `./parquet view --pages -f /shop/orders.parquet` (the `customer_email` chunk of row group 0):

```
     431,374    206,748    column chunk customer_email  SNAPPY   PLAIN/RLE/RLE_DICTIONARY   x 2.7  min=anna.bauer103@example.co  max=zoe.weber99@example.com
     431,374    162,636      DICTIONARY_PAGE PLAIN             17,248 values, header 20 B
     594,010     34,727      DATA_PAGE       RLE_DICTIONARY    20,000 values, header 82 B  [rep 0 B | def 2,799 B | values 32,308 B, bit width 14]
     628,737      9,385      DATA_PAGE       RLE_DICTIONARY     5,000 values, header 82 B  [rep 0 B | def 691 B | values 8,636 B, bit width 15]
```

17,248 distinct e-mail addresses in this row group → dictionary indices need 14–15 bits; 8 % nulls make the
definition levels 2.8 KB instead of 8 bytes (compare `country`: `def 8 B`, indices with 4 bit).

**Parquet vs. HFile.** Both are immutable files with a trailer/footer that is read first and data blocks found via
offsets in it. The HFile ([EDUDFSBASE.md 5.2](EDUDFSBASE.md#52-hfile)) is **row-oriented and sorted by key** with
a block index (B+ tree) for point lookups; Parquet is **column-oriented** with min/max statistics for scans over
few columns. HFile = OLTP-style `get`, Parquet = analytical `SELECT sum(x) … WHERE …`.

**Row groups vs. HDFS blocks.** HDFS cuts the file into blocks without knowing anything about Parquet:

```
HDFS blocks of the file:
  blk_1073741863  [0 .. 1,048,576)          - row groups 0, 1
  blk_1073741864  [1,048,576 .. 2,097,152)  - row groups 1, 2, 3
  blk_1073741865  [2,097,152 .. 3,145,728)  - row groups 3, 4
  blk_1073741866  [3,145,728 .. 4,194,304)  - row groups 4, 5, 6
  blk_1073741867  [4,194,304 .. 5,154,334)  - row groups 6, 7
```

Row group 1 lies in two blocks – possibly on two different DataNodes. Hence the recommendation: row group size =
block size (or an integer fraction of it). `parquet-java` (Spark, Hive) has `parquet.block.size` (row group size, default
128 MB) and pads to block boundaries when needed; pyarrow has no padding.

## 5. Tools

### 5.1 `./parquet view` – look inside (counterpart of `./hbase hfile`)

| Option | Output |
|---|---|
| (none) | layout: offset and size of magic, row groups, column chunks (codec, encodings, compression ratio, min/max), footer; the HDFS blocks of the file and which row groups lie in them |
| `--pages` | in addition every page: type, encoding, number of values, header size; for data pages the split into repetition levels / definition levels / values and the bit width of the dictionary indices |
| `-m` | footer: version, `created_by`, key/value metadata, schema, statistics of every column in every row group |
| `--html out.html` | interactive picture (below) |
| `-f file:local.parquet` | a local file instead of a path in EduDFS |

The row groups and column chunks come from pyarrow (`ParquetFile.metadata`). Pages are not exposed by pyarrow:
`explorer.py` reads them itself – every page starts with a `PageHeader` in the Thrift compact protocol, decoded by the
~60 lines of `ThriftCompact`. For the levels and values the page is decompressed (`pyarrow.decompress`) and the
RLE/bit-packing hybrid, PLAIN and the DELTA_BINARY_PACKED header are decoded in Python (`rle_hybrid`, `plain_values`,
`delta_header`).

### 5.2 The HTML explorer

```bash
./parquet view --html parquet.html -f /shop/orders.parquet
open parquet.html
```

(`./parquet demo 2` writes `parquet.html` as well.) Sections:

1. **The file, byte by byte** – one box per column chunk (colored per column, width ∝ bytes), the footer, the HDFS
   blocks as a ruler with dashed boundaries.
2. **Pushdown simulator** – tick columns (projection) and type a filter (`country = 'DE' and quantity >= 4`). It
   evaluates the footer statistics in the browser and fades everything that would not be read; key figures: row
   groups, columns, column chunks, bytes and **HDFS blocks** read. Four example queries as buttons.
3. **Row groups and column chunks** – nested boxes (row group → column chunk → the first values as pills), the
   picture from the lecture with real data.
4. **Selected column chunk and its pages** – metadata, then the chunk as nested boxes like in the Parquet
   specification: column chunk → page 0, 1, … → *page header (ThriftCompactProtocol)*, *repetition levels*,
   *definition levels*, *values*. Every box shows its size and its decoded content:
   * page header: the Thrift fields (`type`, `compressed_page_size`, `num_values`, `encoding`, statistics, …),
   * repetition levels: "not stored" for flat columns (max repetition level 0),
   * definition levels: the RLE/bit-packed runs and the first 24 levels (0 = null, red) – `RLE 20,000× 1` means a
     column without nulls costs 8 bytes; `customer_email` shows the nulls,
   * values: the dictionary page (index → value); for `RLE_DICTIONARY` the bit width, the runs and the first
     indices with the value they point to; for `PLAIN` the values; for `DELTA_BINARY_PACKED` first value, min
     delta and bit widths (`order_id` in `orders_tuned.parquet`: 4–7 bits per value instead of 64).
     Values are aligned with the rows: a null row has a definition level 0 and no entry in the values.
5. **Footer** – position, version, schema.
6. **Statistics matrix** – min/max of every column in every row group; after a simulation, green/red show which
   condition keeps or rules out a row group.

The simulator does what the reader does with the footer – the measured bytes come from `./parquet query`.

### 5.3 `./parquet query` – real queries with an I/O report

```bash
./parquet query /shop/orders.parquet --columns unit_price --where "order_ts >= '2025-12-01'" --explain
```

```
16,538 rows, columns: unit_price
...
I/O over HttpFS: 3 read requests, 282,101 of 5,154,334 bytes (5.5 %) from 1 of 1 file(s)
  /shop/orders.parquet   5,154,334 B   read 282,101 B in 3 requests | row groups 1/8 | column chunks 2/80 | HDFS blocks 1/5

/shop/orders.parquet: footer statistics of 8 row groups
  row group 0: SKIP   order_ts in [2025-01-01 00:02:14 .. 2025-02-15 10:29:48] >= 2025-12-01 00:00:00? NO
  ...
  row group 7: read   order_ts in [2025-11-15 15:45:37 .. 2025-12-31 06:03:24] >= 2025-12-01 00:00:00? maybe
```

* `--where`: conditions `column op value` joined with `and`; `op` ∈ `= != < <= > >=`; strings and timestamps in quotes.
* `--explain`: the decision per row group from the footer statistics, and for partitioned directories which files
  are skipped by their directory name.
* `--no-pushdown`: read the whole file, filter and project in memory afterwards (the comparison).
* `--sum col`: also the sum of a column.
* The path can be a file (`.parquet`, `.csv`) or a partitioned directory (Hive style `col=value/`).

From Python directly (`./parquet python`):

```python
import datetime as dt
import pyarrow.dataset as ds
from edudfs_fs import edudfs_filesystem, IO

fs = edudfs_filesystem()
orders = ds.dataset("/shop/orders.parquet", filesystem=fs, format="parquet")
t = orders.to_table(columns=["unit_price"], filter=ds.field("order_ts") >= dt.datetime(2025, 12, 1))
print(t.num_rows, IO.requests, IO.bytes_read)
```

## 6. Hands-on, step by step

Each step = `./parquet demo <n>`. Watch the HttpFS terminal while it runs.

### Step 1 – The data set, and why not CSV?

Writes the same table twice: `/shop/orders.csv` (21.11 MB, 22 blocks) and `/shop/orders.parquet` (4.92 MB,
5 blocks, `row_group_size=25_000`):

```python
table = dataset.orders()
pq.write_table(table, "/shop/orders.parquet", filesystem=edudfs_filesystem(), row_group_size=25_000)
```

**Observe:** `./hdfs dfs -ls -R /shop`, `./hdfs dfs -metasave` – the Parquet file is a completely normal HDFS file
with 5 blocks à 3 replicas. HDFS knows nothing about row groups.

### Step 2 – Inside the file

Prints the layout (row group 0 with all column chunks), the statistics of row group 0 as in the lecture code
(`pf.metadata.row_group(0).column(i).statistics`), the HDFS blocks per row group, and writes `parquet.html`.

**Observe:** `order_id` and `order_ts` are by far the biggest chunks (each value distinct → the dictionary is as big
as the data); `country`, `quantity`, `status` are tiny. Then `./parquet view --pages …`: dictionary page + data
pages, definition levels only big for `customer_email` (nulls).

### Step 3 – Projection pushdown

`SELECT unit_price` three times:

| Read | Requests | Bytes |
|---|---|---|
| CSV | 22 | 22,134,289 (100 %) – every line must be read and split |
| Parquet without pushdown | 81 | 5,209,890 (101 %: footer + every chunk) |
| Parquet with projection | 9 | 245,398 (4.8 %): footer + 8 × `unit_price` |

**Observe:** in the HttpFS terminal, 1 request for the footer and 8 requests of ~22 KB at the offsets of the
`unit_price` chunks – one per row group, spread over all 5 blocks.

### Step 4 – Predicate pushdown

| Query | Row groups | Bytes | HDFS blocks |
|---|---|---|---|
| `WHERE order_ts >= '2025-12-01'` | **1 / 8** | 429,284 (8.3 %) | 1 / 5 |
| `WHERE country = 'DE'` | 8 / 8 | 1,524,486 (29.6 %) | 5 / 5 |
| `WHERE status = 'refunded'` (does not exist) | 8 / 8 | 1,289,882 (25.0 %) | 5 / 5 |

**Observe** (`--explain`): the data arrives in time order, every row group covers ~6 weeks → 7 of 8 row groups are
skipped without being read. For `country` every row group has `min = AT, max = US` – `'DE'` lies in every range,
nothing can be skipped. `'refunded'` lies between `'cancelled'` and `'shipped'`: min/max cannot prove that a value is
**absent** – that is the job of a Bloom filter ("definitely not in this row group"). Bloom filters are **optional
and off by default** – no writer creates them unless you enable them per column:

* Spark (parquet-java): `df.write.option("parquet.bloom.filter.enabled#customer_email", "true")
  .option("parquet.bloom.filter.expected.ndv#customer_email", "50000")`. When reading, Spark uses Bloom filters
  that are present in the file automatically (`parquet.filter.bloom.enabled`, default `true`).
* Databricks/Delta have their own switches (`spark.databricks.io.skipping.bloomFilter.enabled`,
  `CREATE BLOOMFILTER INDEX`) – not part of Apache Spark.
* pyarrow 26 can write them (`bloom_filter_options={"status": {"ndv": 10, "fpp": 0.01}}`), but its dataset reader
  does not use them to skip row groups – the same 1,289,882 bytes are read.

### Step 5 – ORDER BY before writing

`/shop/orders_sorted.parquet` = the same data, `sort_by([("country", …), ("order_ts", …)])`, with `sorting_columns`
in the footer.

```
row group 0: read   country in [AT .. DE] = DE? maybe
row group 1: read   country in [DE .. DE] = DE? maybe
row group 2: read   country in [DE .. DE] = DE? maybe
row group 3: read   country in [DE .. FR] = DE? maybe
row group 4: SKIP   country in [FR .. FR] = DE? NO
...
```

`country = 'DE'`: 4 / 8 row groups, 747,042 bytes instead of 1,524,486. Side effect – long runs of equal values
compress extremely well: the `country` column shrinks from **101,682 to 992 bytes**, `city` from 128,843 to 78,840.

**Observe:** only one sort order per file – try `--where "order_ts >= '2025-12-01'"` on the sorted file
(now every row group spans the whole year).

### Step 6 – Column partitioning

```python
pq.write_to_dataset(table, "/shop/orders_by_country", filesystem=fs, partition_cols=["country"])
```

```
/shop/orders_by_country/country=AT/part-0.parquet     410,166 B
/shop/orders_by_country/country=DE/part-0.parquet   1,494,554 B
...
/shop/orders_by_country/country=US/part-0.parquet     260,591 B
```

The column `country` is no longer stored in the files – its value is the directory name. `WHERE country = 'DE'`
opens **1 of 9 files** (partition pruning, decided from the directory listing alone); combined with
`order_ts >= '2025-12-01'`, the row group statistics inside that file prune further (2 / 3 row groups).

**Observe:** 8 of 9 files are smaller than one HDFS block. With 128 MB blocks and a partition per day (365 × 9
directories) this becomes the *small files problem*: every file costs a NameNode object, a footer read and a task.
Partition by low-cardinality columns that (almost) every query filters on; compact small files.

### Step 7 – Encodings and compression

Each column written alone with different encodings and codecs (bytes on disk, all 200,000 values):

| Column | codec | PLAIN | DICTIONARY (default) | other encoding |
|---|---|---:|---:|---|
| `order_id` (1, 2, 3, …) | snappy | 802,509 | 1,177,406 | **1,630** `DELTA_BINARY_PACKED` |
| `order_ts` (ascending) | snappy | 1,177,083 | 1,552,730 | **482,901** `DELTA_BINARY_PACKED` |
| `country` (9 values) | snappy | 363,364 | **101,682** | |
| `product` (48 strings) | snappy | 765,958 | **157,194** | 875,439 `DELTA_BYTE_ARRAY` |
| `customer_email` (~40,000) | snappy | 1,764,604 | 1,646,365 | **1,548,554** `DELTA_LENGTH_BYTE_ARRAY` |
| `unit_price` (75 doubles) | snappy | 494,179 | **179,862** | 1,073,372 `BYTE_STREAM_SPLIT` |

(The demo prints `none`, `snappy` and `zstd` for every row.) Everything together – sorted by country, delta for
`order_id`/`order_ts`, zstd: `/shop/orders_tuned.parquet` = **2.37 MB** instead of 4.92 MB.

```python
pq.write_table(sorted_table, "/shop/orders_tuned.parquet", filesystem=fs, compression="zstd",
               use_dictionary=["country", "city", "category", "product", "quantity", "unit_price",
                               "customer_email", "status"],
               column_encoding={"order_id": "DELTA_BINARY_PACKED", "order_ts": "DELTA_BINARY_PACKED"})
```

### Step 8 – Best practices

Summary, see [8](#8-best-practices).

## 6a. Timing it: the benchmark web UI

The steps above count bytes. The web UI measures the **time** a user waits – the same query on CSV and on
differently organized Parquet data, and two sweeps to find the sweet spots.

```bash
./parquet web
open http://localhost:8050/
```

(Minicluster and HttpFS must be running.) `http://localhost:8050/#run` runs everything once by itself – handy in the
lecture. Missing layouts are written to EduDFS on the first run (`/bench/…`; the 3,300-file layout takes ~30 s).

**Controls** (one row, above everything):

* **Query** – four aggregates with different selectivity:

  | Query | SQL | What it favors |
  |---|---|---|
  | Total revenue 2025 | `SELECT sum(unit_price * quantity)` | projection only, nothing to skip |
  | Revenue per country | `… GROUP BY country` | projection; partitions are read completely |
  | Germany in December, per category | `… WHERE country = 'DE' AND order_ts >= '2025-12-01' GROUP BY category` | predicate pushdown and partition pruning |
  | One order by id | `SELECT * … WHERE order_id = 123456` | point lookup: sorted `order_id` → one row group |

* **Storage** – on localhost a request costs microseconds, so small files would look harmless. The presets add
  a wait per request and a bandwidth limit (`edudfs_fs.STORAGE`), for metadata and data requests alike:
  `local` (none), `hdfs` (2 ms, 200 MB/s – a cluster in a data center), `s3` (25 ms, 50 MB/s – an object store over
  the internet). This is a **simulation**, labeled as such in the UI.
* **Repetitions** – median of 1/3/5 runs (layouts with more than 200 files run once).

**Section 1 – CSV vs. Parquet layouts.** One bar per layout, with bytes read, data + metadata requests and files
opened. If one bar is more than 3× the next one, the axis is cut (the bar ends in a gap, its value is printed). Measured
with “Germany in December” and the `hdfs` preset:

| Layout | Time | Read | Requests (data + meta) | Files |
|---|---:|---:|---:|---:|
| CSV | 685 ms | 24.1 MB of 21.1 MB | 25 + 3 | 1 / 1 |
| Parquet, time order | **59 ms** | 371 KB of 4.9 MB | 7 + 4 | 1 / 1 |
| Parquet, shuffled | 266 ms | 2.2 MB of 5.2 MB | 42 + 4 | 1 / 1 |
| Parquet, ORDER BY country | 112 ms | 829 KB of 4.8 MB | 17 + 4 | 1 / 1 |
| Partitioned by country | 118 ms | 411 KB of 4.8 MB | 10 + 14 | 2 / 9 |
| Partitioned by country + day | **21.3 s** | 3.1 MB of 17.4 MB | 497 + 3,666 | 369 / 3,289 |

What to see: CSV reads more than its size (pyarrow reads the beginning twice to infer the column types).
Shuffled data defeats predicate pushdown. The time-ordered file wins here because `order_ts >= December` skips 7 of 8
row groups; sorted by country only skips half. Partitioned by country opens 2 files – the DE file and one more,
whose footer pyarrow reads to learn the schema. Partitioned by day: 3,289 files of ~5 KB, 3.5× the data volume
(every file has its own footer and dictionaries), thousands of listing calls – **360× slower** than one file. The
filter on `order_ts` does not prune `day=` directories, because the filter is on a different column.

**Section 2 – sweet spots.** Each sweep runs with the current storage preset; switch the preset and run again –
every preset becomes its own line (`ms` = absolute, `× best` = relative to the best point of each line, to compare
shapes). Measured with “Germany in December”, `hdfs`:

| Files | 1 | 4 | 16 | 64 | 256 | 1,024 |
|---|---:|---:|---:|---:|---:|---:|
| time | 80 ms | 129 ms | 177 ms | 412 ms | 1.5 s | 6.0 s |
| read | 380 KB | 576 KB | 1.3 MB | 4.4 MB | 6.7 MB | 10.3 MB |

| Rows per row group | 1,000 | 5,000 | 25,000 | 100,000 | 200,000 |
|---|---:|---:|---:|---:|---:|
| time | 738 ms | 206 ms | **89 ms** | 131 ms | 187 ms |
| read | 736 KB | 328 KB | 380 KB | 1.1 MB | 2.0 MB |

* **Files:** the curve only rises – every file costs a listing entry, a footer read and its own requests, and
  1,024 files hold 10 MB instead of 5 MB. In production the lower end is set by parallelism (a file or row group
  is the unit of work), hence files of 128 MB – 1 GB.
* **Row groups:** U-shaped. 200,000 rows = 1 row group → nothing can be skipped, 2 MB read. 1,000 rows → 200 row
  groups, a footer of 200 × 10 column chunk entries and 89 requests. The sweet spot (25,000 rows ≈ 640 KB here) is
  where a row group is small enough to be skipped but big enough that its overhead does not dominate. With
  `s3` (25 ms per request) it moves to bigger row groups – try it. With “Total revenue” (nothing to skip) the right
  half of the U flattens out: only the overhead remains.

The numbers vary from machine to machine; the shapes do not.

## 6b. Nested data: repetition and definition levels

The orders so far are flat: every field holds one value, so every page has **no repetition levels** and the
definition levels only say "null or not". Real data often has lists and structs in one field – an order with its
items, tags, packages. A separate job shows how Parquet stores them:

```bash
./parquet nested                 # writes /shop/orders_nested.parquet, explains the levels, writes parquet_nested.html
open parquet_nested.html         # click a column chunk of tags, items or packages
```

**The data** (`nested.py`, 20,000 orders, row groups of 10,000; the first four rows cover every case):

```text
order_id  customer  tags (list<string>)   items (list<struct<product, qty, unit_price>>)       packages (list<list<string>>)
1         anna      [gift, express]       [{Wireless Mouse, 2, 24.9}]                          [[Wireless Mouse]]
2         ben       []                    [{Desk Lamp, 1, 34.9}, {Coffee Mug, 2, 9.9}]         [[Desk Lamp], [Coffee Mug, Coffee Mug]]
3         clara     null                  []                                                   []
4         david     [gift, null]          null                                                 null
```

**Only leaves become columns** (Dremel encoding). 5 Arrow fields become 7 leaf columns – one column chunk per leaf in
every row group. A struct becomes one leaf per field; a list adds the nodes `list` (repeated) and `element`:

| Leaf column | Path (repetition) | max rep | max def |
|---|---|---|---|
| `order_id`, `customer` | optional | 0 | 1 |
| `tags.list.element` | tags (optional) → list (repeated) → element (optional) | 1 | 3 |
| `items.list.element.product` (`.qty`, `.unit_price`) | items → list (repeated) → element → product | 1 | 4 |
| `packages.list.element.list.element` | packages → list (repeated) → element → list (repeated) → element | 2 | 5 |

**Two numbers per entry** rebuild the nesting – both stored RLE/bit-packed in front of the values of a data page:

* **Repetition level** – `0` = the entry starts a new row; `k` = it is the next element of the k-th list on the path.
* **Definition level** – how many optional/repeated nodes of the path are present. So a null list, an empty list and
  a null element each get their own number; only an entry with def = max has a value in the values section.

As the job prints it – read back from the first data page in EduDFS, not computed:

```text
leaf column tags.list.element   (max rep 1, max def 3)
 row  rep  def  value                meaning
   0    0    3  gift                 value
        1    3  express              next element of tags: value
   1    0    1                       tags = [] (empty list)
   2    0    0                       tags is null
   3    0    3  gift                 value
        1    2                       next element of tags: tags[i] is null

leaf column packages.list.element.list.element   (max rep 2, max def 5)
 row  rep  def  value                meaning
   0    0    5  Wireless Mouse       value
   1    0    5  Desk Lamp            value
        1    5  Coffee Mug           next element of packages: value
        2    5  Coffee Mug           next element of packages[i]: value
   2    0    1                       packages = [] (empty list)
   3    0    0                       packages is null
```

What to notice:

* An empty or null list still takes **one entry** (rows 2 and 3) – otherwise the row would vanish.
* `items.product`, `items.qty` and `items.unit_price` have **identical levels**: every field of a struct repeats the
  structure of the list. Reading only `items.product` needs only that one column chunk (projection pushdown works per
  leaf).
* The levels are cheap: in `packages` the definition levels are almost one RLE run (`RLE 50,167× 5`, 12 bytes),
  the repetition levels need 2 bits per entry.
* `num_values` in the page header counts **level entries**, not rows, and `null_count` counts every entry without a
  value (null list, empty list, null element).

In the HTML picture (section 4) a nested column shows the first rows as records, the meaning of every rep/def level,
the rep levels in their own box, and a table that aligns row, rep level, def level and value – `(0)` marks an entry
that continues row 0, `[ ]` an empty list.

## 7. Encodings and codecs (reference)

| Encoding | How | Good for | In the demo |
|---|---|---|---|
| `PLAIN` | values one after another (int: fixed width, strings: 4-byte length + bytes) | fallback | dictionary pages are PLAIN |
| `RLE_DICTIONARY` (`PLAIN_DICTIONARY`) | dictionary page with the distinct values; data pages hold the indices, RLE/bit-packed with the minimal bit width (1 byte bit width in front) | few distinct values | `country` 4 bit, `status` 2 bit, `quantity` 3 bit |
| `RLE` / bit-packing hybrid | runs of equal values as (count, value), otherwise groups of 8 values packed with *n* bits | levels, dictionary indices, booleans | definition levels; sorted `country` → 992 bytes |
| `DELTA_BINARY_PACKED` | first value, then deltas; per block the minimum delta is subtracted and the rest bit-packed | ascending ints, timestamps | `order_id`: all deltas 1 → ~0 bits |
| `DELTA_LENGTH_BYTE_ARRAY` | all lengths delta-encoded, then the bytes back to back | strings of similar length | `customer_email` |
| `DELTA_BYTE_ARRAY` | prefix length shared with the previous value + suffix ("incremental encoding") | sorted strings, URLs, paths | `product` (unsorted: bad) |
| `BYTE_STREAM_SPLIT` | byte *k* of all floats together, then compression | noisy float measurements | `unit_price` (few values: dictionary wins) |

pyarrow writes dictionary encoding by default and falls back to PLAIN for a chunk when the dictionary exceeds
`dictionary_pagesize_limit` (1 MB). Codecs (`compression=`): `none`, `snappy` (default, fast), `gzip`, `brotli`, `lz4`,
`zstd` (better ratio, `compression_level`). Encoding first, codec second – they multiply.

## 8. Best practices

1. **Columnar + footer statistics:** select only the columns you need; filters on columns with statistics.
2. **Sort** by the column you filter on most – otherwise min/max covers everything and nothing is skipped. Bonus:
   better encoding.
3. **Partition** by a low-cardinality column that almost every query filters on. Too many partitions → small files.
4. **Sizes:** row group ≈ HDFS block size (or a fraction), files of 128 MB to 1 GB (≈ 120,000+ rows per row group);
   compact small files.
5. **Encodings:** dictionary for few values, delta for sorted numbers/timestamps, then a codec (snappy/zstd).
6. **Bloom filters** (off by default, enable per column when writing) for selective equality filters on
   high-cardinality columns (ids, e-mails): min/max cannot prove absence.

## 9. Exercises

1. **Row group size.** Write `/shop/orders.parquet` with `row_group_size=10_000` and `=200_000`. How do the size,
   the footer size and the bytes read for `order_ts >= '2025-12-01'` change? What is the trade-off?
2. **Align to blocks.** Find a `row_group_size` so that each row group is just below 1 MB. With `./parquet view`:
   how many row groups now lie in two HDFS blocks?
3. **Which sort order?** Sort by `order_ts` only, by `country` only, by `(country, order_ts)`. Compare bytes read for
   `country = 'DE'`, `order_ts >= '2025-12-01'` and both together.
4. **Range filter on a dictionary column.** Why does `WHERE status = 'returned'` never skip a row group, even in the
   sorted file? What would help?
5. **Partition by month.** Add a column `month` and partition by it. How many files, how big? Query December.
   Then partition by `(country, month)` and count the files below 1 MB.
6. **Look at a page.** In `./parquet view --pages`, find a data page of `quantity`. Values: 20,000. Bit width: 3.
   Estimate the size of the indices and compare with the printed value size. Why is it smaller?
7. **Projection on CSV.** Why can a CSV reader not skip the other columns, even if it only needs `unit_price`?
8. **Footer first.** Using the HttpFS log, list the first three requests of a query. Why is the first one always at the
   end of the file?

## 10. The same with Spark (optional)

Not needed for this hands-on. On a real Hadoop cluster (or local files) Spark shows the same effects:

```python
df = spark.read.parquet("hdfs://namenode:8020/shop/orders.parquet")
q = df.select("unit_price").where("order_ts >= '2025-12-01'")
q.explain()   # FileScan parquet ... PushedFilters: [IsNotNull(order_ts), GreaterThanOrEqual(order_ts,...)],
              #                      ReadSchema: struct<unit_price:double,order_ts:timestamp>
```

`ReadSchema` = projection pushdown, `PushedFilters` = predicate pushdown, `PartitionFilters` = partition pruning;
the Spark UI shows "number of files read" and "size of files read". Spark cannot connect to EduDFS (it speaks the
Hadoop RPC protocol, not EduDFS's), so here pyarrow plays the reader.

## 11. Simplifications

| Here | In production |
|---|---|
| pyarrow reads through a Python file system over the HttpFS REST gateway | `libhdfs`/`HadoopFileSystem` (JNI) or WebHDFS directly to the DataNodes |
| `pre_buffer=False`: one request per column chunk | `pre_buffer=True`: neighboring ranges merged, fewer requests |
| the whole file is buffered in memory before `CREATE` | streaming write |
| block size 1 MB, files of a few MB | 128/256 MB blocks, files of 128 MB – 1 GB |
| statistics at row group level | also the **page index** (`ColumnIndex`/`OffsetIndex`, `write_page_index=True`) for page-level skipping |
| Bloom filters: pyarrow writes them (`bloom_filter_options`) but does not skip with them | off by default everywhere; Spark/parquet-java: enabled per column when writing, then used automatically when reading |

## 12. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `HttpFS not reachable at http://localhost:14000/…` | start `./hdfs httpfs` (and before it `./hdfs minicluster 3`) |
| `First run: ./parquet setup` | `.venv/` is missing |
| `./parquet setup` fails | Python without `venv` (Debian/Ubuntu: `apt install python3-venv`) or no network for `pip` |
| `Address already in use` from HttpFS | another HttpFS is running; port: `./hdfs httpfs 14001` and `export EDUDFS_HTTPFS=http://localhost:14001/webhdfs/v1` |
| `/shop/... does not exist` after a restart | the NameNode keeps the namespace only in memory ([DOCUMENTATION.md 8.8](DOCUMENTATION.md#88-restart-what-survives)) – run the demo again |
| `--where` cannot be read | `column op value`, conditions joined with `and`, strings in quotes: `"country = 'DE'"` |

## 13. References

* Apache Parquet format specification – <https://parquet.apache.org/docs/file-format/>, `parquet.thrift`
  (FileMetaData, PageHeader) and `Encodings.md` in <https://github.com/apache/parquet-format>
* Apache Arrow, Python: reading and writing Parquet – <https://arrow.apache.org/docs/python/parquet.html>,
  datasets (filters, partitioning) – <https://arrow.apache.org/docs/python/dataset.html>
* WebHDFS REST API – <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>
* Dremio, *Tuning Parquet* – <https://www.dremio.com/blog/tuning-parquet/>
