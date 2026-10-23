package edudfsbase.io.hfile;

import edudfsbase.Cell;
import edudfsbase.HBaseConfiguration;
import edudfsbase.KeyValue;
import edudfsbase.codec.KeyValueCodec;
import edudfsbase.fs.HFileSystem;
import edudfsbase.io.PositionedReadable;
import edudfsbase.util.BloomFilterChunk;
import edudfsbase.util.BloomFilterUtil;
import edudfsbase.util.Bytes;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter ({@code hbase hfile}): looks
 * inside an HFile. The flags of the original plus some EduDFSBase views.
 * <pre>
 *   ./hbase hfile [options] -f &lt;path in HDFS | file:local-path&gt;
 *     -m          meta: trailer, FileInfo, Bloom filter          (original)
 *     -b          block index: offset, size, first key per block  (original)
 *     -h          block headers of all blocks in file order       (original)
 *     -p          cells as "K: ... V: ..."                        (original)
 *     -e          keys only (with -p)                             (original)
 *     -s          statistics: rows, columns, cells per type        (original, extended)
 *     -r row      only this row (with -p/--table); with --bloom: what does the filter say for the row?
 *     -v          verbose = -m -b -s                              (original: verbose output)
 *     --layout    physical map of the file: which block lies where, by section (EduDFSBase)
 *     --tree      the block index as a tree: root -&gt; intermediate -&gt; leaf -&gt; data blocks (EduDFSBase)
 *     --offset n  the block at / containing byte n, every line with its byte offset (EduDFSBase)
 *     --dump      the whole file, every line with line number and byte offset - next to an editor (EduDFSBase)
 *     --html out  interactive picture of the file: block map, B+ tree, lookup of a row (EduDFSBase)
 *     --table     the cells as a table, with the data block they are in (EduDFSBase)
 *     --grid      "wide column" view: one line per row, one column per family:qualifier (EduDFSBase)
 *     --bloom     the Bloom filter's bit array as a picture (EduDFSBase)
 * </pre>
 * Without options: --layout -m (-m contains a shortened --tree).
 */
public class HFilePrettyPrinter {

    private final PrintStream out;
    private boolean printMeta;
    private boolean printBlockIndex;
    private boolean printBlockHeaders;
    private boolean printKeyValues;
    private boolean printKeysOnly;
    private boolean printStats;
    private boolean printLayout;
    private boolean printTable;
    private boolean printGrid;
    private boolean printBloom;
    private boolean printTree;
    private boolean printDump;
    private long offset = -1;
    private String htmlFile;
    private byte[] row;

    public HFilePrettyPrinter(PrintStream out) {
        this.out = out;
    }

    public static void main(String[] args) throws Exception {
        Log.quiet = !Boolean.getBoolean("edudfs.verbose");
        System.exit(new HFilePrettyPrinter(System.out).run(args));
    }

    public int run(String[] args) throws IOException {
        List<String> files = new ArrayList<>();
        boolean any = false;
        for (int i = 0; i < args.length; i++) {
            any |= args[i].startsWith("-") && !args[i].equals("-f") && !args[i].equals("-r");
            switch (args[i]) {
                case "-m" -> printMeta = true;
                case "-b" -> printBlockIndex = true;
                case "-h" -> printBlockHeaders = true;
                case "-p" -> printKeyValues = true;
                case "-e" -> printKeysOnly = true;
                case "-s" -> printStats = true;
                case "-v" -> printMeta = printBlockIndex = printStats = true;
                case "--layout" -> printLayout = true;
                case "--table" -> printTable = true;
                case "--grid" -> printGrid = true;
                case "--bloom" -> printBloom = true;
                case "--tree" -> printTree = true;
                case "--dump" -> printDump = true;
                case "--offset" -> offset = Long.parseLong(args[++i]);
                case "--html" -> htmlFile = args[++i];
                case "-r" -> row = Bytes.toBytesBinary(args[++i]);
                case "-f" -> files.add(args[++i]);
                default -> {
                    if (args[i].startsWith("-")) {
                        usage();
                        return 1;
                    }
                    files.add(args[i]);
                }
            }
        }
        if (files.isEmpty()) {
            usage();
            return 1;
        }
        if (!any) {
            printLayout = printMeta = true;
        }
        HFileSystem fs = null;
        try {
            List<String> expanded = new ArrayList<>();
            for (String f : files) {
                if (f.contains("*") && !f.startsWith("file:")) {
                    if (fs == null) {
                        fs = new HFileSystem();
                    }
                    List<String> matches = glob(fs, f);
                    if (matches.isEmpty()) {
                        out.println("ERROR: no HFile matches " + f);
                        return 1;
                    }
                    expanded.addAll(matches);
                } else {
                    expanded.add(f);
                }
            }
            if (htmlFile != null && expanded.size() > 1) {
                out.println("ERROR: --html needs exactly one HFile, " + expanded.size() + " match: " + expanded);
                return 1;
            }
            for (String f : expanded) {
                PositionedReadable in;
                if (f.startsWith("file:")) {
                    in = PositionedReadable.of(new File(f.substring(5)));
                } else {
                    if (fs == null) {
                        fs = new HFileSystem();
                    }
                    in = fs.open(f);
                }
                try (HFileReaderImpl reader = (HFileReaderImpl) HFile.createReader(f, in, null)) {
                    print(reader);
                }
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        return 0;
    }

    /**
     * EduDFSBase: '*' in an HDFS path, e.g. /hbase/data/default/TestTable/&#42;/info/&#42; - so that the commands in
     * the README work without copying the random region and file names. Hidden entries (.tmp, .regioninfo) are
     * skipped; the last component only matches files.
     */
    private static List<String> glob(HFileSystem fs, String pattern) throws IOException {
        List<String> current = new ArrayList<>(List.of(""));
        String[] parts = pattern.substring(1).split("/");
        for (int i = 0; i < parts.length; i++) {
            boolean last = i == parts.length - 1;
            List<String> next = new ArrayList<>();
            for (String dir : current) {
                if (!parts[i].contains("*")) {
                    next.add(dir + "/" + parts[i]);
                    continue;
                }
                if (!fs.exists(dir.isEmpty() ? "/" : dir)) {
                    continue;
                }
                String regex = ("\\Q" + parts[i] + "\\E").replace("*", "\\E.*\\Q");
                for (HdfsFileStatus st : fs.listStatus(dir.isEmpty() ? "/" : dir)) {
                    String name = st.getPath().substring(st.getPath().lastIndexOf('/') + 1);
                    if (!name.startsWith(".") && name.matches(regex) && st.isDir() != last) {
                        next.add(st.getPath());
                    }
                }
            }
            current = next;
        }
        List<String> files = new ArrayList<>();
        for (String f : current) {
            if (fs.exists(f) && !fs.getFileStatus(f).isDir()) {
                files.add(f);
            }
        }
        java.util.Collections.sort(files);
        return files;
    }

    private void usage() {
        out.println("Usage: hbase hfile [-m] [-b] [-h] [-p [-e]] [-s] [-r row] [-v] [--layout] [--tree] [--offset n]"
                + " [--dump] [--html out.html] [--table] [--grid] [--bloom] -f <path in HDFS | file:local-path>");
        out.println("  The HDFS path may contain '*': -f '/hbase/data/default/TestTable/*/info/*' (quote it)");
        out.println("  -m meta (trailer, FileInfo, index tree, Bloom)   -b data block index   -h block headers");
        out.println("  -p cells (-e keys only)   -s statistics   -r row: only this row   -v = -m -b -s");
        out.println("  --layout  which block lies at which offset, by section");
        out.println("  --tree    the block index (B+ tree) completely: root -> intermediate -> leaf -> data blocks");
        out.println("  --offset  the block at / containing this byte offset, every line with its offset");
        out.println("  --dump    the whole file with line numbers and byte offsets (to read next to an editor)");
        out.println("  --html    write an interactive HTML picture of the file (open it in a browser)");
        out.println("  --table / --grid  cells as a table / as a wide-column grid   --bloom  Bloom bit array");
    }

    private void print(HFileReaderImpl r) throws IOException {
        out.println("Scanning -> " + r.getName() + "  (" + r.length() + " bytes)");
        HFileBlockMap map = new HFileBlockMap(r);
        if (printLayout) {
            printLayout(r, map);
        }
        if (printTree) {
            out.println();
            printTree(r, map, true);
        }
        if (offset >= 0) {
            printOffset(r, map, offset);
        }
        if (printDump) {
            printDump(r, map);
        }
        if (printBlockHeaders) {
            printBlockHeaders(map);
        }
        if (printMeta) {
            printMeta(r, map);
        }
        if (printBlockIndex) {
            printBlockIndex(r, map);
        }
        if (printKeyValues || printTable || printGrid || printStats) {
            List<Cell> cells = new ArrayList<>();
            List<Integer> blocks = new ArrayList<>();
            List<HFileBlockIndex.Entry> idx = r.getDataBlockEntries();
            for (int b = 0; b < idx.size(); b++) {
                for (KeyValue kv : r.readBlock(idx.get(b).getOffset(), false, null).getCells()) {
                    if (row == null || Bytes.equals(row, kv.getRow())) {
                        cells.add(kv);
                        blocks.add(b);
                    }
                }
            }
            if (printKeyValues) {
                for (Cell c : cells) {
                    out.println(printKeysOnly ? "K: " + c : KeyValueCodec.encode(c));
                }
            }
            if (printTable) {
                printTable(cells, blocks);
            }
            if (printGrid) {
                printGrid(cells);
            }
            if (printStats) {
                printStats(r, cells);
            }
        }
        if (printBloom) {
            printBloom(r);
        }
        if (htmlFile != null) {
            new HFileHtmlExporter(r, map).write(new File(htmlFile));
            out.println();
            out.println("HTML picture written to " + new File(htmlFile).getAbsolutePath() + " - open it in a browser.");
        }
    }

    // ------------------------------------------------------------- layout
    private void printLayout(HFileReaderImpl r, HFileBlockMap map) {
        out.println();
        out.println("Physical layout (offset = byte position in the file):");
        out.printf("  %8s %7s  %-9s %s%n", "offset", "bytes", "magic", "content");
        HFileBlockMap.Section section = null;
        for (HFileBlockMap.Item i : map.getItems()) {
            if (i.section != section) {
                section = i.section;
                String title = "-------- " + section.title + " (" + section.description + ") ";
                out.println("  " + title + "-".repeat(Math.max(4, 100 - title.length())));
            }
            out.printf("  %8d %7d  %-9s %s%n", i.offset, i.size, i.type.getMagic(), i.description);
        }
        out.println("  Every block = header (" + HFileBlock.HEADER_SIZE + " bytes: magic, onDiskSizeWithoutHeader,"
                + " prevBlockOffset, checksum) + body. File size " + r.length() + " bytes.");
    }

    private void printBlockHeaders(HFileBlockMap map) {
        out.println();
        out.println("Block headers:");
        for (HFileBlockMap.Item i : map.getItems()) {
            if (i.block != null) {
                out.printf("  @%-8d %s onDiskSizeWithoutHeader=%d prevBlockOffset=%d%n", i.offset, i.type.getMagic(),
                        i.block.getOnDiskSizeWithoutHeader(), i.block.getPrevBlockOffset());
            }
        }
    }

    // --------------------------------------------------------------- tree
    /**
     * The block index as a tree. full = false (for -m): the root lists all its entries but only the first and
     * the last are expanded; below the root only the first and the last entry of each block are shown.
     */
    private void printTree(HFileReaderImpl r, HFileBlockMap map, boolean full) throws IOException {
        FixedFileTrailer t = r.getTrailer();
        int levels = t.getNumDataIndexLevels();
        out.println("Block index as a B+ tree (numDataIndexLevels=" + levels + ", " + switch (levels) {
            case 1 -> "root -> data blocks";
            case 2 -> "root -> leaf -> data blocks";
            default -> "root -> " + "intermediate -> ".repeat(levels - 2) + "leaf -> data blocks";
        } + "; hfile.index.block.max.size=" + HBaseConfiguration.INDEX_BLOCK_MAX_SIZE + "):");
        HFileBlockMap.Item root = map.at(t.getLoadOnOpenDataOffset());
        out.println("  " + BlockType.ROOT_INDEX.getMagic() + " @" + root.offset + "  (root index, load-on-open section"
                + " - always in memory, " + r.getDataBlockIndexReader().getRootBlockCount() + " entries)");
        printChildren(r, map, r.getDataBlockIndexReader().getRootEntries(), 1, levels, "  ", full);
        if (!full && levels > 1) {
            out.println("  (shortened - the whole tree: --tree, as a picture: --html out.html)");
        }
    }

    private void printChildren(HFileReaderImpl r, HFileBlockMap map, List<HFileBlockIndex.Entry> entries, int level,
                               int levels, String indent, boolean full) throws IOException {
        int n = entries.size();
        for (int k = 0; k < n; k++) {
            if (!full && level > 1 && n > 3 && k == 1) {
                out.println(indent + "├── ... " + (n - 2) + " more entries ...");
                k = n - 1;
            }
            HFileBlockIndex.Entry e = entries.get(k);
            boolean last = k == n - 1;
            HFileBlockMap.Item child = map.at(e.getOffset());
            String target = child == null ? "? @" + e.getOffset()
                    : child.type.getMagic() + " @" + e.getOffset() + "  (" + child.role
                    + (child.type == BlockType.DATA ? ", " + child.size + " bytes" : ", " + child.block.getIndexEntries().size()
                    + " entries") + ")";
            out.println(indent + (last ? "└── " : "├── ") + "[" + k + "] " + KeyValueCodec.encodeKey(e.getFirstKey())
                    + "  ->  " + target);
            if (level < levels && child != null && (full || k == 0 || last)) {
                printChildren(r, map, child.block.getIndexEntries(), level + 1, levels,
                        indent + (last ? "    " : "│   "), full);
            }
        }
    }

    // ------------------------------------------------------------- offset
    /** The block at or containing the offset, every line with its byte offset; the line with the offset marked. */
    private void printOffset(HFileReaderImpl r, HFileBlockMap map, long off) throws IOException {
        out.println();
        HFileBlockMap.Item i = map.containing(off);
        if (i == null) {
            out.println("Offset " + off + " is outside the file (0.." + (r.length() - 1) + ").");
            return;
        }
        out.println(off == i.offset ? "Offset " + off + " is the start of " + i.type.getMagic() + " (" + i.role + ", "
                + i.size + " bytes, " + i.section.title + ")."
                : "Offset " + off + " lies inside " + i.type.getMagic() + " @" + i.offset + " (" + i.role + ", "
                + i.size + " bytes) - byte " + (off - i.offset) + " of the block.");
        out.println("  " + i.description);
        byte[] b = new byte[i.size];
        r.getInput().readFully(i.offset, b, 0, i.size);
        String text = new String(b, StandardCharsets.US_ASCII);
        out.printf("  %8s  %s%n", "offset", "content");
        long pos = i.offset;
        boolean first = true;
        for (String line : text.split("\n", -1)) {
            long lineEnd = pos + line.length() + 1;
            if (pos >= i.end()) {
                break;
            }
            String mark = off >= pos && off < lineEnd ? " >" : "  ";
            String shown = line.isBlank() && line.length() > 20 ? "(" + line.length() + " spaces padding)" : line;
            out.printf("%s%8d  %s%n", mark, pos, shown);
            if (first && i.type != BlockType.TRAILER) {
                out.printf("  %8s  %s%n", "", "^ header: magic = block type, onDiskSizeWithoutHeader = bytes of the body,"
                        + " prevBlockOffset = previous " + i.type.getMagic() + " block (-1 = none), checksum = CRC32"
                        + " of the body");
            } else if (i.type != BlockType.TRAILER) {
                String hint = explainLine(map, i, line);
                if (hint != null) {
                    out.printf("  %8s  %s%n", "", hint);
                }
            }
            first = false;
            pos = lineEnd;
        }
    }

    /** For index entries: what the offset points to. */
    private static String explainLine(HFileBlockMap map, HFileBlockMap.Item i, String line) {
        if (line.startsWith("offset=")) {
            long target = Long.parseLong(line.substring(7, line.indexOf(' ')));
            return "^ child: " + map.nameOf(target) + " (its first key follows firstKey=)";
        }
        if (line.startsWith(HFileBlockIndex.NUM_ENTRIES)) {
            return "^ number of entries (original: int + \"secondary index\" with the position of every entry)";
        }
        if (line.startsWith(HFileBlockIndex.MID_KEY)) {
            return "^ mid-key metadata: leaf block and entry of the middle data block (region split point)";
        }
        return null;
    }

    // --------------------------------------------------------------- dump
    /** The whole file line by line: line number (as in an editor), byte offset, text; block starts marked. */
    private void printDump(HFileReaderImpl r, HFileBlockMap map) throws IOException {
        out.println();
        out.println("Whole file (line = line number in an editor, offset = byte position):");
        byte[] b = new byte[(int) r.length()];
        r.getInput().readFully(0, b, 0, b.length);
        String text = new String(b, StandardCharsets.US_ASCII);
        out.printf("  %6s %8s  %s%n", "line", "offset", "content");
        long pos = 0;
        int lineNo = 1;
        for (String line : text.split("\n", -1)) {
            if (pos >= b.length) {
                break;
            }
            HFileBlockMap.Item i = map.at(pos);
            String shown = line.isBlank() && line.length() > 20 ? "(" + line.length() + " spaces padding)" : line;
            if (i != null) {
                out.printf("  %6s %8s  ---- %s @%d: %s%n", "", "", i.type.getMagic(), i.offset, i.description);
            }
            out.printf("  %6d %8d  %s%n", lineNo++, pos, shown);
            pos += line.length() + 1;
        }
    }

    // --------------------------------------------------------------- meta
    private void printMeta(HFileReaderImpl r, HFileBlockMap map) throws IOException {
        out.println();
        out.println("Trailer (" + FixedFileTrailer.TRAILER_SIZE + " bytes at offset " + (r.length()
                - FixedFileTrailer.TRAILER_SIZE) + " = file size - " + FixedFileTrailer.TRAILER_SIZE + "):");
        for (Map.Entry<String, String> e : r.getTrailer().toMap().entrySet()) {
            String kv = e.getKey() + "=" + e.getValue();
            out.printf("    %-42s %s%n", kv, explainTrailer(e.getKey(), e.getValue(), map));
        }
        out.printf("    %-42s %s%n", "majorVersion=" + FixedFileTrailer.MAJOR_VERSION + " minorVersion="
                + FixedFileTrailer.MINOR_VERSION, "(HFile format version 3, last line of the trailer)");
        out.println("Fileinfo:");
        for (Map.Entry<String, String> e : r.getHFileInfo().asMap().entrySet()) {
            out.println("    " + e.getKey() + " = " + e.getValue() + explainFileInfo(e.getKey()));
        }
        KeyValue mid = r.midKey();
        out.println("Mid-key: " + (mid == null ? "-" : KeyValueCodec.encodeKey(mid))
                + "   (first key of the middle data block - a region split would happen at this row)");
        out.println();
        printTree(r, map, false);
        out.println();
        BloomFilterChunk bloom = r.getGeneralBloomFilter();
        if (bloom == null) {
            out.println("Bloom filter:\n    Not present");
        } else {
            out.println("Bloom filter:");
            out.println("    BloomSize: " + bloom.getBytes().length + " bytes (" + bloom.getBitSize() + " bits)");
            out.println("    No of Keys in bloom: " + bloom.getKeyCount() + " (planned for " + bloom.getMaxKeys()
                    + ")");
            out.println("    Max Keys for bloom: " + bloom.getMaxKeys());
            out.println("    Number of hash functions: " + bloom.getHashCount());
            out.println("    Hash type: MURMUR_HASH");
            out.printf(Locale.ROOT, "    Bits set: %.1f%%%n", 100 * bloom.fillRatio());
            out.printf(Locale.ROOT, "    Expected false positive rate: %.3f%% with %d keys (%.2f%% if all %d"
                            + " planned keys had arrived; target io.storefile.bloom.error.rate=%.1f%%)%n",
                    100 * bloom.actualErrorRate(),
                    bloom.getKeyCount(),
                    100 * BloomFilterUtil.actualErrorRate(bloom.getMaxKeys(), bloom.getBitSize(), bloom.getHashCount()),
                    bloom.getMaxKeys(), 100 * HBaseConfiguration.BLOOM_ERROR_RATE);
        }
    }

    /** What a trailer field means; for offsets: which block lies there. */
    private static String explainTrailer(String key, String value, HFileBlockMap map) {
        String target = "";
        if (key.endsWith("Offset")) {
            HFileBlockMap.Item i = map.at(Long.parseLong(value));
            target = i == null ? " -> (no block)" : " -> " + i.type.getMagic() + " " + i.role;
        }
        return "(" + switch (key) {
            case "fileInfoOffset" -> "where the FileInfo block is" + target;
            case "loadOnOpenDataOffset" -> "start of the load-on-open section = ROOT INDEX OFFSET" + target
                    + "; read from here to the trailer when the file is opened";
            case "dataIndexCount" -> "entries in the ROOT level of the data index (= data blocks only with 1 level)";
            case "uncompressedDataIndexSize" -> "bytes of all data index blocks: leaf + intermediate + root";
            case "metaIndexCount" -> "entries of the meta block index - 0: no META blocks";
            case "totalUncompressedBytes" -> "all blocks + trailer before compression (no compression: = file size)";
            case "entryCount" -> "number of cells in the file";
            case "compressionCodec" -> "compression of the blocks (original: NONE, SNAPPY, LZ4, ZSTD, GZ)";
            case "numDataIndexLevels" -> "height of the index tree: 1 = root -> data, 2 = root -> leaf -> data,"
                    + " 3 = root -> intermediate -> leaf -> data";
            case "firstDataBlockOffset" -> "first data block" + target + "; a full scan starts here";
            case "lastDataBlockOffset" -> "last data block" + target + "; a scan stops after it";
            case "comparatorClassName" -> "sort order: row, family, qualifier, timestamp descending, type";
            default -> "";
        } + ")";
    }

    private static String explainFileInfo(String key) {
        return switch (key) {
            case HFileInfo.MAX_SEQ_ID_KEY -> "   (all edits up to this sequence ID are in this file - WAL replay"
                    + " skips them)";
            case HFileInfo.TIMERANGE_KEY -> "   (smallest..largest timestamp)";
            case HFileInfo.MAJOR_COMPACTION_KEY -> "   (true: no delete markers left)";
            case HFileInfo.LASTKEY -> "   (last key of the file; the first key is the first root index entry)";
            default -> "";
        };
    }

    private void printBlockIndex(HFileReaderImpl r, HFileBlockMap map) throws IOException {
        out.println();
        List<HFileBlockIndex.Entry> entries = r.getDataBlockEntries();
        int levels = r.getTrailer().getNumDataIndexLevels();
        out.println("Data block index (" + entries.size() + " data blocks" + (levels == 1 ? ", all in the root"
                : ", read from the leaf level of a " + levels + "-level index; --tree shows the tree") + "):");
        out.printf("  %5s %8s %7s  %s%n", "block", "offset", "bytes", "first key");
        int i = 0;
        for (HFileBlockIndex.Entry e : entries) {
            out.printf("  %5d %8d %7d  %s%n", i++, e.getOffset(), e.getOnDiskSize(),
                    KeyValueCodec.encodeKey(e.getFirstKey()));
        }
        out.println("  A get for row R reads only the block whose first key is the last one < R: binary search in the"
                + " root, then in one block per level.");
    }

    // ------------------------------------------------------- table, grid
    private void printTable(List<Cell> cells, List<Integer> blocks) {
        out.println();
        out.println("Cells as a table (sorted as in the file):");
        String fmt = "  %-14s %-16s %-14s %-12s %5s %5s  %s%n";
        out.printf(fmt, "row", "family:qualifier", "timestamp", "type", "seqid", "block", "value");
        for (int i = 0; i < cells.size(); i++) {
            Cell c = cells.get(i);
            out.printf(fmt, cut(Bytes.toStringBinary(c.getRow()), 14),
                    cut(Bytes.toStringBinary(c.getFamily()) + ":" + Bytes.toStringBinary(c.getQualifier()), 16),
                    c.getTimestamp(), c.getType(), c.getSequenceId(), blocks.get(i),
                    c.getType().isDelete() ? "" : cut(Bytes.toStringBinary(c.getValue()), 30));
        }
    }

    /**
     * "Wide column" view: one line per row, one column per family:qualifier that occurs in the file.
     * Empty fields: the row has no such column (sparse - costs no space). Several versions: newest
     * value, then "(+n)". Delete markers: [Delete...].
     */
    private void printGrid(List<Cell> cells) {
        TreeSet<String> columns = new TreeSet<>();
        Map<String, Map<String, List<Cell>>> rows = new LinkedHashMap<>();
        for (Cell c : cells) {
            String col = Bytes.toStringBinary(c.getFamily()) + ":" + Bytes.toStringBinary(c.getQualifier());
            columns.add(col);
            rows.computeIfAbsent(Bytes.toStringBinary(c.getRow()), k -> new TreeMap<>())
                    .computeIfAbsent(col, k -> new ArrayList<>()).add(c);
        }
        out.println();
        out.println("Logical view (one line per row, one column per family:qualifier; empty = column not present):");
        StringBuilder head = new StringBuilder(String.format("  %-14s", "row"));
        for (String col : columns) {
            head.append(String.format(" | %-16s", cut(col, 16)));
        }
        out.println(head);
        out.println("  " + "-".repeat(head.length() - 2));
        for (Map.Entry<String, Map<String, List<Cell>>> r : rows.entrySet()) {
            StringBuilder line = new StringBuilder(String.format("  %-14s", cut(r.getKey(), 14)));
            for (String col : columns) {
                List<Cell> versions = r.getValue().get(col);
                String v = "";
                if (versions != null) {
                    Cell newest = versions.get(0);
                    v = newest.getType().isDelete() ? "[" + newest.getType() + "]"
                            : Bytes.toStringBinary(newest.getValue());
                    if (versions.size() > 1) {
                        v = cut(v, 11) + " (+" + (versions.size() - 1) + ")";
                    }
                }
                line.append(String.format(" | %-16s", cut(v, 16)));
            }
            out.println(line);
        }
    }

    private static String cut(String s, int n) {
        return s.length() <= n ? s : s.substring(0, n - 1) + "~";
    }

    // -------------------------------------------------------------- stats
    private void printStats(HFileReaderImpl r, List<Cell> cells) throws IOException {
        Map<String, Integer> perType = new TreeMap<>();
        TreeSet<String> rowSet = new TreeSet<>();
        TreeSet<String> colSet = new TreeSet<>();
        long keyLen = 0;
        long valLen = 0;
        int maxRowCells = 0;
        String maxRow = "";
        Map<String, Integer> cellsPerRow = new TreeMap<>();
        for (Cell c : cells) {
            perType.merge(c.getType().name(), 1, Integer::sum);
            String rk = Bytes.toStringBinary(c.getRow());
            rowSet.add(rk);
            colSet.add(Bytes.toStringBinary(c.getFamily()) + ":" + Bytes.toStringBinary(c.getQualifier()));
            keyLen += ((KeyValue) c).getKeyLength();
            valLen += c.getValue().length;
            int n = cellsPerRow.merge(rk, 1, Integer::sum);
            if (n > maxRowCells) {
                maxRowCells = n;
                maxRow = rk;
            }
        }
        out.println();
        out.println("Stats:");
        out.println("    cells: " + cells.size() + " " + perType);
        out.println("    rows: " + rowSet.size() + ", distinct columns: " + colSet.size() + " " + colSet);
        out.println("    largest row: " + maxRow + " (" + maxRowCells + " cells)");
        if (!cells.isEmpty()) {
            out.println("    avg key length: " + keyLen / cells.size() + " bytes, avg value length: "
                    + valLen / cells.size() + " bytes (binary layout of the original)");
        }
        out.println("    data blocks: " + r.getDataBlockCount() + ", index levels: "
                + r.getTrailer().getNumDataIndexLevels() + ", file size: "
                + r.length() + " bytes");
    }

    // -------------------------------------------------------------- bloom
    private void printBloom(HFileReaderImpl r) {
        BloomFilterChunk bloom = r.getGeneralBloomFilter();
        out.println();
        if (bloom == null) {
            out.println("No Bloom filter (BLOOMFILTER => 'NONE').");
            return;
        }
        out.println("Bloom filter bit array (" + bloom.getBitSize() + " bits, '#' = 1, '.' = 0, 64 per line):");
        byte[] b = bloom.getBytes();
        StringBuilder line = new StringBuilder();
        for (long bit = 0; bit < bloom.getBitSize(); bit++) {
            line.append((b[(int) (bit >> 3)] & (1 << (bit & 7))) != 0 ? '#' : '.');
            if (line.length() == 64) {
                out.println("  " + line);
                line.setLength(0);
            }
        }
        if (line.length() > 0) {
            out.println("  " + line);
        }
        if (row != null) {
            String type = r.getHFileInfo().get(HFileInfo.BLOOM_FILTER_TYPE_KEY);
            if ("ROW".equals(type)) {
                out.println("Bloom check for row '" + Bytes.toStringBinary(row) + "': "
                        + (bloom.contains(row) ? "MAYBE contained" : "definitely NOT contained"));
            } else {
                out.println("Bloom type " + type + ": the key is row+qualifier, a row alone cannot be checked here.");
            }
        }
    }
}
