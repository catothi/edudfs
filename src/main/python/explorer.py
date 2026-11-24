"""Look inside a Parquet file - the counterpart of ./hbase hfile for HFiles.

    ./parquet view -f /shop/orders.parquet                 layout: magic, row groups, column chunks, footer
    ./parquet view --pages -f /shop/orders.parquet         ... down to the pages (page header, levels, values)
    ./parquet view -m -f /shop/orders.parquet              footer: schema and min/max per row group
    ./parquet view --html parquet.html -f /shop/orders.parquet   interactive picture incl. pushdown simulator
    ./parquet view -f file:local.parquet                   a local file instead of one in EduDFS

The row groups and column chunks come from the footer (pyarrow's FileMetaData). The pages are not
exposed by pyarrow, so they are read here directly: every page starts with a PageHeader in Thrift
compact protocol, which the small decoder below understands.
"""

import argparse
import datetime as dt
import html
import io
import json
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq

import edudfs_fs

PAGE_TYPES = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}
ENCODINGS = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED", 5: "DELTA_BINARY_PACKED",
             6: "DELTA_LENGTH_BYTE_ARRAY", 7: "DELTA_BYTE_ARRAY", 8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}


# --------------------------------------------------------------------------- Thrift compact protocol
class ThriftCompact:
    """Just enough of the Thrift compact protocol to read a PageHeader: structs come back as {field id: value}."""

    def __init__(self, buf, pos=0):
        self.buf, self.pos = buf, pos

    def byte(self):
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def varint(self):
        shift = result = 0
        while True:
            b = self.byte()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, t):
        if t in (1, 2):                       # BOOLEAN_TRUE / BOOLEAN_FALSE (inside lists: one byte)
            return t == 1
        if t == 3:                            # BYTE
            return self.byte()
        if t in (4, 5, 6):                    # I16, I32, I64
            return self.zigzag()
        if t == 7:                            # DOUBLE
            self.pos += 8
            return struct.unpack_from("<d", self.buf, self.pos - 8)[0]
        if t == 8:                            # BINARY
            n = self.varint()
            self.pos += n
            return bytes(self.buf[self.pos - n:self.pos])
        if t in (9, 10):                      # LIST, SET
            h = self.byte()
            n, et = h >> 4, h & 0x0F
            if n == 15:
                n = self.varint()
            return [self.value(et) for _ in range(n)]
        if t == 11:                           # MAP
            n = self.varint()
            kv = self.byte() if n else 0
            return {self.value(kv >> 4): self.value(kv & 0x0F) for _ in range(n)}
        if t == 12:                           # STRUCT
            return self.struct()
        raise ValueError(f"unknown Thrift type {t} at {self.pos}")

    def struct(self):
        fields, last = {}, 0
        while True:
            h = self.byte()
            if h == 0:                        # STOP
                return fields
            delta, t = h >> 4, h & 0x0F
            fid = last + delta if delta else self.zigzag()
            fields[fid] = self.value(t)
            last = fid


# --------------------------------------------------------------------------- reading the file
def load(path):
    """(bytes, name, HDFS blocks or None) for 'file:<local>' or a path in EduDFS."""
    if path.startswith("file:"):
        with open(path[5:], "rb") as f:
            return f.read(), path[5:], None
    return edudfs_fs.read_all(path), path, edudfs_fs.block_locations(path)


SAMPLE = 24                                   # values / levels shown per page in the HTML picture
FIXED = {"INT32": "<i", "INT64": "<q", "FLOAT": "<f", "DOUBLE": "<d"}


def convert(v, physical, arrow_type):
    """A raw physical value -> readable value (timestamps are INT64 on disk)."""
    if physical == "BYTE_ARRAY":
        return v.decode("utf-8", errors="replace")
    if isinstance(v, bytes):
        return v.hex()
    return pretty(pa.scalar(v).cast(arrow_type).as_py()) if pa.types.is_timestamp(arrow_type) else pretty(v)


def decode_stat(raw, physical, arrow_type):
    """A min/max value from a page header (plain encoded bytes) -> readable value."""
    if raw is None:
        return None
    v = struct.unpack(FIXED[physical], raw)[0] if physical in FIXED else raw
    return convert(v, physical, arrow_type)


def pretty(v):
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, float):
        return round(v, 4)
    return v


def plain_values(data, pos, physical, limit=None):
    """PLAIN encoding: fixed-width little endian numbers, or [4 byte length][bytes] for BYTE_ARRAY."""
    res = []
    while pos < len(data) and (limit is None or len(res) < limit):
        if physical == "BYTE_ARRAY":
            n = struct.unpack_from("<i", data, pos)[0]
            res.append(bytes(data[pos + 4:pos + 4 + n]))
            pos += 4 + n
        elif physical in FIXED:
            res.append(struct.unpack_from(FIXED[physical], data, pos)[0])
            pos += struct.calcsize(FIXED[physical])
        else:
            break
    return res


def rle_hybrid(data, pos, end, bit_width, total):
    """RLE / bit-packing hybrid (levels and dictionary indices). Returns the runs and the first values.
    Run header (varint): lowest bit 0 = RLE run (count = header >> 1, then the value once),
                         lowest bit 1 = bit-packed run (header >> 1 groups of 8 values, bit_width bits each)."""
    runs, first, first_run, seen, start, header_bytes = [], [], [], 0, pos, 0
    width = (bit_width + 7) // 8
    while pos < end and seen < total:
        t = ThriftCompact(data, pos)
        header = t.varint()
        head = t.pos - pos
        header_bytes += head
        pos = t.pos
        if header & 1:
            groups = header >> 1
            count = min(groups * 8, total - seen)
            need = min(count, SAMPLE - len(first))
            if need > 0:
                bits = int.from_bytes(data[pos:pos + (need * bit_width + 7) // 8], "little")
                first += [(bits >> (i * bit_width)) & ((1 << bit_width) - 1) for i in range(need)]
            bits = int.from_bytes(data[pos:pos + bit_width], "little")      # the first 8 values of the run
            head8 = [(bits >> (i * bit_width)) & ((1 << bit_width) - 1) for i in range(min(8, count))]
            pos += groups * bit_width
            runs.append(["bit-packed", count, None, head + groups * bit_width, head8])
        else:
            count = min(header >> 1, total - seen)
            v = int.from_bytes(data[pos:pos + width], "little")
            pos += width
            first += [v] * max(0, min(count, SAMPLE - len(first)))
            runs.append(["RLE", count, v, head + width])
        first_run += [len(runs) - 1] * (len(first) - len(first_run))
        seen += count
    return {"runs": runs[:SAMPLE], "num_runs": len(runs), "rle_runs": sum(r[0] == "RLE" for r in runs),
            "first": first, "first_run": first_run, "bit_width": bit_width, "values": seen, "bytes": pos - start,
            "rle_values": sum(r[1] for r in runs if r[0] == "RLE"),
            "rle_bytes": sum(r[3] for r in runs if r[0] == "RLE"), "header_bytes": header_bytes}


def delta_header(data, pos):
    """DELTA_BINARY_PACKED: <block size> <miniblocks per block> <total count> <first value>, then per block
    <min delta> <bit width per miniblock> <miniblocks of (delta - min delta)>."""
    t = ThriftCompact(data, pos)
    h = {"block_size": t.varint(), "miniblocks": t.varint(), "total": t.varint(), "first_value": t.zigzag()}
    if h["total"] > 1:
        h["min_delta"] = t.zigzag()
        h["bit_widths"] = list(data[t.pos:t.pos + h["miniblocks"]])
    return h


def body(buf, start, h, codec):
    """The page data after the header, decompressed (None if the codec is unknown to pyarrow)."""
    raw = bytes(buf[start:start + h[3]])
    if codec == "UNCOMPRESSED":
        return raw
    try:
        return pa.decompress(raw, decompressed_size=h[2], codec=codec.lower()).to_pybytes()
    except (pa.ArrowException, ValueError):
        return None


def header_fields(h):
    """The PageHeader struct (Thrift field ids) with the names from parquet.thrift."""
    f = [["type", PAGE_TYPES.get(h[1], h[1])], ["uncompressed_page_size", h[2]], ["compressed_page_size", h[3]]]
    if 4 in h:
        f.append(["crc", h[4]])
    if 7 in h:
        d = h[7]
        f += [["dictionary_page_header.num_values", d[1]], ["dictionary_page_header.encoding", ENCODINGS.get(d[2])]]
    if 5 in h:
        d = h[5]
        f += [["data_page_header.num_values", d[1]], ["data_page_header.encoding", ENCODINGS.get(d[2])],
              ["data_page_header.definition_level_encoding", ENCODINGS.get(d[3])],
              ["data_page_header.repetition_level_encoding", ENCODINGS.get(d[4])]]
    if 8 in h:
        d = h[8]
        f += [["data_page_header_v2.num_values", d[1]], ["data_page_header_v2.num_nulls", d[2]],
              ["data_page_header_v2.encoding", ENCODINGS.get(d[4])]]
    return f


def read_pages(buf, start, length, codec, physical, arrow_type, max_def, max_rep):
    """Walks the pages of one column chunk: [{offset, header, size, type, encoding, ...}]."""
    pages, pos, dictionary = [], start, None
    while pos < start + length:
        t = ThriftCompact(buf, pos)
        h = t.struct()
        header_len = t.pos - pos
        p = {"o": pos, "header": header_len, "type": PAGE_TYPES.get(h[1], str(h[1])),
             "uncompressed": h[2], "compressed": h[3], "fields": header_fields(h)}
        data = body(buf, t.pos, h, codec)
        if 7 in h:                            # DictionaryPageHeader
            d = h[7]
            p.update(values=d[1], encoding=ENCODINGS.get(d[2], str(d[2])))
            if data is not None:
                dictionary = plain_values(data, 0, physical)
                p["dict"] = [convert(v, physical, arrow_type) for v in dictionary[:SAMPLE]]
        elif 5 in h:                          # DataPageHeader (v1)
            d = h[5]
            p.update(values=d[1], encoding=ENCODINGS.get(d[2], str(d[2])),
                     def_encoding=ENCODINGS.get(d[3]), rep_encoding=ENCODINGS.get(d[4]),
                     max_def=max_def, max_rep=max_rep)
            st = d.get(5, {})
            if st:
                p["min"] = decode_stat(st.get(6, st.get(2)), physical, arrow_type)
                p["max"] = decode_stat(st.get(5, st.get(1)), physical, arrow_type)
                p["nulls"] = st.get(3)
            if data is not None:
                p.update(split_v1_page(data, max_def, max_rep, p["encoding"], d[1], p.get("nulls") or 0,
                                       physical, arrow_type, dictionary))
        elif 8 in h:                          # DataPageHeaderV2: levels are stored uncompressed before the values
            d = h[8]
            p.update(values=d[1], nulls=d[2], encoding=ENCODINGS.get(d[4], str(d[4])),
                     rep_bytes=d[6], def_bytes=d[5], value_bytes=h[2] - d[5] - d[6])
        pages.append(p)
        pos = t.pos + h[3]
    return pages


def split_v1_page(data, max_def, max_rep, encoding, num_values, nulls, physical, arrow_type, dictionary):
    """Inside a v1 data page (after decompression): [rep levels][def levels][values].
    Each level section is RLE/bit-packed and prefixed with its length (4 bytes, little endian);
    a level section is missing if its max level is 0 (rep: flat column, def: required column)."""
    pos, res = 0, {"rep_bytes": 0, "def_bytes": 0}
    for key, max_level in (("rep", max_rep), ("def", max_def)):
        if max_level > 0:
            n = struct.unpack_from("<i", data, pos)[0]
            res[key + "_bytes"] = 4 + n
            res[key + "_levels"] = rle_hybrid(data, pos + 4, pos + 4 + n, max_level.bit_length(), num_values)
            pos += 4 + n
    res["value_bytes"] = len(data) - pos
    if encoding in ("RLE_DICTIONARY", "PLAIN_DICTIONARY") and pos < len(data):
        res["bit_width"] = data[pos]          # dictionary indices: 1 byte bit width, then RLE/bit-packed runs
        idx = rle_hybrid(data, pos + 1, len(data), data[pos], num_values - nulls)
        idx["mapped"] = [convert(dictionary[i], physical, arrow_type) if dictionary and i < len(dictionary) else None
                         for i in idx["first"]]
        res["indices"] = idx
    elif encoding == "PLAIN":
        res["plain"] = [convert(v, physical, arrow_type) for v in plain_values(data, pos, physical, SAMPLE)]
    elif encoding == "DELTA_BINARY_PACKED":
        res["delta"] = delta_header(data, pos)
        if pa.types.is_timestamp(arrow_type):
            res["delta"]["first_value_as"] = convert(res["delta"]["first_value"], physical, arrow_type)
    else:
        res["hex"] = data[pos:pos + 48].hex(" ")
    return res


def leaves(arr, path=""):
    """Depth-first leaf arrays of a (nested) Arrow column - the order of the Parquet column chunks.
    Lists are flattened, so a leaf holds the values of all list elements one after another."""
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    t = arr.type
    if pa.types.is_struct(t):
        return [x for i in range(t.num_fields) for x in leaves(arr.field(i), f"{path}.{t.field(i).name}")]
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return leaves(arr.flatten(), path + ".list.element")
    return [(path.lstrip("."), arr)]


def schema_nodes(data, footer_offset):
    """The schema tree from the footer (FileMetaData field 2: list<SchemaElement>), as the path of nodes
    [(name, repetition)] of every leaf column. repetition: REQUIRED / OPTIONAL / REPEATED."""
    elements = ThriftCompact(data, footer_offset).struct()[2]
    reps = {0: "REQUIRED", 1: "OPTIONAL", 2: "REPEATED"}
    res, pos = [], 1

    def walk(prefix, n_children):
        nonlocal pos
        for _ in range(n_children):
            e = pos and elements[pos]
            pos += 1
            node = prefix + [(e[4].decode(), reps.get(e.get(3, 0)))]
            if e.get(5):                      # num_children: a group
                walk(node, e[5])
            else:
                res.append(node)
    walk([], elements[0].get(5, 0))
    return res


def level_meanings(nodes):
    """What each definition / repetition level of a leaf means (Dremel encoding), walking the path:
    def level d = the first d OPTIONAL/REPEATED nodes are present (below them: null or an empty list);
    rep level 0 = a new row starts, r = k = the next element of the k-th list on the path."""
    d, r, cur, idx = [], ["new row"], "", iter("ijkl")
    for name, rep in nodes:
        if rep == "REPEATED":                 # the 'list' node of a LIST: one entry per element
            d.append(f"{cur} = [] (empty list)")
            r.append(f"next element of {cur}")
            cur += f"[{next(idx)}]"
            continue
        if name != "element":
            cur = f"{cur}.{name}" if cur else name
        if rep == "OPTIONAL":
            d.append(f"{cur} is null")
    d.append("value")
    return {"def": d, "rep": r, "path": [f"{n} ({rep.lower()})" for n, rep in nodes]}


def analyze(path, with_pages=True, samples=6):
    data, name, blocks = load(path)
    if data[:4] != b"PAR1" or data[-4:] != b"PAR1":
        raise SystemExit(f"{name} is not a Parquet file (magic 'PAR1' missing)")
    pf = pq.ParquetFile(io.BytesIO(data))
    md = pf.metadata
    footer_len = struct.unpack("<i", data[-8:-4])[0]
    footer_offset = len(data) - 8 - footer_len
    # one entry per leaf column (= per column chunk); for flat files these are just the column names
    leaf_types = [arr.type for _, arr in leaves(pf.read_row_group(0, use_threads=False).to_struct_array())] \
        if md.num_row_groups else []
    columns = [pf.schema.column(i).path for i in range(md.num_columns)]
    nested = any(pf.schema.column(i).max_repetition_level for i in range(md.num_columns)) or \
        any(len(n) > 1 for n in schema_nodes(data, footer_offset))
    info = {"name": name, "size": len(data), "blocks": blocks or [], "num_rows": md.num_rows,
            "created_by": md.created_by, "format_version": md.format_version, "footer_offset": footer_offset,
            "footer_len": footer_len, "columns": columns, "schema": str(pf.schema).split("\n", 1)[1].strip(),
            "arrow_types": [str(t) for t in leaf_types], "nested": nested,
            "kv": {k.decode(): (v[:60] + b"...").decode(errors="replace") if len(v) > 60 else v.decode(errors="replace")
                   for k, v in (md.metadata or {}).items()},
            "row_groups": []}
    if nested:
        info["levels"] = {c: level_meanings(n) for c, n in zip(columns, schema_nodes(data, footer_offset))}
    for r in range(md.num_row_groups):
        rg = md.row_group(r)
        sample_leaves = [arr for _, arr in leaves(pf.read_row_group(r).slice(0, samples).to_struct_array())] \
            if samples else None
        cols = []
        for c in range(rg.num_columns):
            cc = rg.column(c)
            start = cc.dictionary_page_offset if cc.has_dictionary_page else cc.data_page_offset
            st = cc.statistics
            col = {"name": cc.path_in_schema, "o": start, "len": cc.total_compressed_size,
                   "uncompressed": cc.total_uncompressed_size, "physical": cc.physical_type,
                   "encodings": list(cc.encodings), "codec": cc.compression, "values": cc.num_values,
                   "min": pretty(st.min) if st is not None and st.has_min_max else None,
                   "max": pretty(st.max) if st is not None and st.has_min_max else None,
                   "nulls": st.null_count if st is not None and st.has_null_count else None,
                   "sample": [pretty(v) for v in sample_leaves[c].to_pylist()[:samples]] if samples else [],
                   # ColumnMetaData in the footer: where the reader starts walking the pages of this chunk
                   "meta": {"dictionary_page_offset": cc.dictionary_page_offset if cc.has_dictionary_page else None,
                            "data_page_offset": cc.data_page_offset, "total_compressed_size": cc.total_compressed_size,
                            "num_values": cc.num_values, "offset_index": cc.has_offset_index}}
            if with_pages:
                sc = pf.schema.column(c)
                col["pages"] = read_pages(memoryview(data), start, cc.total_compressed_size, cc.compression,
                                          cc.physical_type, leaf_types[c],
                                          sc.max_definition_level, sc.max_repetition_level)
            cols.append(col)
        info["row_groups"].append({"i": r, "rows": rg.num_rows, "bytes": rg.total_byte_size,
                                   "o": cols[0]["o"], "len": sum(c["len"] for c in cols), "cols": cols})
        if nested:                            # the first rows as records, to compare with the levels of the pages
            head = pf.read_row_group(r).slice(0, 8)
            info["row_groups"][-1]["records"] = {
                f: [json.dumps(v, default=str, ensure_ascii=False) for v in head.column(f).to_pylist()]
                for f in head.column_names}
    return info


# --------------------------------------------------------------------------- terminal output
def fmt(n):
    return f"{n:,}"


def print_layout(info, pages, out=sys.stdout, chunks_of=None):
    """chunks_of=n: list the column chunks only for the first n row groups (shorter output)."""
    w = out.write
    w(f"Parquet file {info['name']}\n  {fmt(info['size'])} bytes, {fmt(info['num_rows'])} rows, "
      f"{len(info['row_groups'])} row groups, {len(info['columns'])} columns, written by {info['created_by']}\n\n")
    w(f"{'offset':>12} {'size':>10}  section\n")
    w(f"{0:>12} {4:>10}  magic 'PAR1'\n")
    for rg in info["row_groups"]:
        w(f"{fmt(rg['o']):>12} {fmt(rg['len']):>10}  row group {rg['i']}  ({fmt(rg['rows'])} rows)\n")
        for c in rg["cols"] if chunks_of is None or rg["i"] < chunks_of else []:
            ratio = c["uncompressed"] / c["len"] if c["len"] else 0
            w(f"{fmt(c['o']):>12} {fmt(c['len']):>10}    column chunk {c['name']:<15} {c['codec']:<8} "
              f"{'/'.join(c['encodings']):<26} x{ratio:4.1f}  min={c['min']!s:.24}  max={c['max']!s:.24}\n")
            for p in c.get("pages", []) if pages else []:
                levels = ""
                if "def_bytes" in p:
                    levels = f"  [rep {fmt(p['rep_bytes'])} B | def {fmt(p['def_bytes'])} B | values {fmt(p['value_bytes'])} B" \
                             + (f", bit width {p['bit_width']}" if "bit_width" in p else "") + "]"
                w(f"{fmt(p['o']):>12} {fmt(p['header'] + p['compressed']):>10}      {p['type']:<15} "
                  f"{p['encoding']:<16} {fmt(p['values']):>7} values, header {p['header']} B{levels}\n")
    if chunks_of is not None and len(info["row_groups"]) > chunks_of:
        w(f"{'':>12} {'':>10}    (column chunks of row groups {chunks_of}.. omitted, ./parquet view lists all)\n")
    w(f"{fmt(info['footer_offset']):>12} {fmt(info['footer_len']):>10}  footer (FileMetaData, Thrift compact)\n")
    w(f"{fmt(info['size'] - 8):>12} {4:>10}  footer length = {fmt(info['footer_len'])}\n")
    w(f"{fmt(info['size'] - 4):>12} {4:>10}  magic 'PAR1'\n")
    if info["blocks"]:
        w("\nHDFS blocks of the file:\n")
        for b in info["blocks"]:
            rgs = [str(rg["i"]) for rg in info["row_groups"]
                   if rg["o"] < b["offset"] + b["length"] and rg["o"] + rg["len"] > b["offset"]]
            w(f"  {b['block']}  [{fmt(b['offset'])} .. {fmt(b['offset'] + b['length'])})  on {', '.join(b['names'])}"
              f"  - row groups {', '.join(rgs) or '-'}\n")


def print_meta(info, out=sys.stdout):
    w = out.write
    w(f"Footer of {info['name']} ({fmt(info['footer_len'])} bytes at offset {fmt(info['footer_offset'])})\n")
    w(f"  format version {info['format_version']}, created_by {info['created_by']}, {fmt(info['num_rows'])} rows\n")
    w(f"  key/value metadata: {', '.join(info['kv']) or '-'}\n\n{info['schema']}\n\n")
    w("Statistics per row group (what a reader checks BEFORE it reads any data):\n")
    for rg in info["row_groups"]:
        w(f"  row group {rg['i']}: {fmt(rg['rows'])} rows\n")
        for c in rg["cols"]:
            w(f"    {c['name']:<15} min={c['min']!s:<28.28} max={c['max']!s:<28.28} nulls={c['nulls']}\n")


# --------------------------------------------------------------------------- HTML
def write_html(info, filename):
    with open(__file__.rsplit("/", 1)[0] + "/explorer.html", encoding="utf-8") as f:
        template = f.read()
    data = json.dumps(info, default=str, separators=(",", ":")).replace("</", "<\\/")
    page = template.replace("/*DATA*/null", data).replace("{{TITLE}}", html.escape(info["name"]))
    with open(filename, "w", encoding="utf-8") as f:
        f.write(page)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="./parquet view", description="Look inside a Parquet file in EduDFS")
    ap.add_argument("-f", "--file", required=True, help="path in EduDFS, or file:<local path>")
    ap.add_argument("--pages", action="store_true", help="also list the pages of every column chunk")
    ap.add_argument("-m", "--meta", action="store_true", help="print the footer: schema and statistics")
    ap.add_argument("--html", metavar="OUT", help="write an interactive HTML picture of the file")
    a = ap.parse_args(argv)
    info = analyze(a.file, with_pages=True, samples=6)
    if a.meta:
        print_meta(info)
    else:
        print_layout(info, a.pages)
    if a.html:
        write_html(info, a.html)
        print(f"\nHTML picture written to {a.html} - open it in a browser.")


if __name__ == "__main__":
    main()
