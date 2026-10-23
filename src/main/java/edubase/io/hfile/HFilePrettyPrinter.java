package edubase.io.hfile;

import edubase.Cell;
import edubase.HBaseConfiguration;
import edubase.KeyValue;
import edubase.codec.KeyValueCodec;
import edubase.fs.HFileSystem;
import edubase.io.PositionedReadable;
import edubase.util.BloomFilterChunk;
import edubase.util.BloomFilterUtil;
import edubase.util.Bytes;
import edudfs.common.Log;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter ({@code hbase hfile}): looks
 * inside an HFile. The flags of the original plus some EduBase views.
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
 *     --layout    physical map of the file: which block lies where (EduBase)
 *     --table     the cells as a table, with the data block they are in (EduBase)
 *     --grid      "wide column" view: one line per row, one column per family:qualifier (EduBase)
 *     --bloom     the Bloom filter's bit array as a picture (EduBase)
 * </pre>
 * Without options: --layout -m.
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
            any |= args[i].startsWith("-") && !args[i].equals("-f") && !args[i].equals("-r");
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
            for (String f : files) {
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

    private void usage() {
        out.println("Usage: hbase hfile [-m] [-b] [-h] [-p [-e]] [-s] [-r row] [-v] [--layout] [--table] [--grid]"
                + " [--bloom] -f <path in HDFS | file:local-path>");
    }

    private void print(HFileReaderImpl r) throws IOException {
        out.println("Scanning -> " + r.getName() + "  (" + r.length() + " bytes)");
        if (printLayout) {
            printLayout(r);
        }
        if (printBlockHeaders) {
            printBlockHeaders(r);
        }
        if (printMeta) {
            printMeta(r);
        }
        if (printBlockIndex) {
            printBlockIndex(r);
        }
        if (printKeyValues || printTable || printGrid || printStats) {
            List<Cell> cells = new ArrayList<>();
            List<Integer> blocks = new ArrayList<>();
            List<HFileBlockIndex.Entry> idx = r.getDataBlockIndexReader().getEntries();
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
    }

    // ------------------------------------------------------------- layout
    private void printLayout(HFileReaderImpl r) throws IOException {
        out.println();
        out.println("Physical layout (offset = byte position in the file):");
        out.printf("  %8s %7s  %-9s %s%n", "offset", "bytes", "magic", "content");
        long offset = 0;
        int dataBlock = 0;
        FixedFileTrailer t = r.getTrailer();
        long end = r.length() - FixedFileTrailer.TRAILER_SIZE;
        boolean loadOnOpenShown = false;
        while (offset < end) {
            if (offset == t.getLoadOnOpenDataOffset() && !loadOnOpenShown) {
                out.println("  -------- load-on-open section (read completely when the file is opened) --------");
                loadOnOpenShown = true;
            }
            HFileBlock b = HFileBlock.read(r.getInput(), offset, r.getName());
            String content = switch (b.getBlockType()) {
                case DATA -> {
                    List<KeyValue> cells = b.getCells();
                    yield "data block #" + dataBlock++ + ": " + cells.size() + " cells, "
                            + Bytes.toStringBinary(cells.get(0).getRow()) + " .. "
                            + Bytes.toStringBinary(cells.get(cells.size() - 1).getRow());
                }
                case BLOOM_CHUNK -> {
                    BloomFilterChunk bloom = r.getGeneralBloomFilter();
                    yield "Bloom bit array: " + bloom.getBitSize() + " bits, " + bloom.getKeyCount() + " keys, "
                            + String.format(Locale.ROOT, "%.0f%% of bits set", 100 * bloom.fillRatio());
                }
                case ROOT_INDEX -> "block index: " + b.getLines().size()
                        + " entries (first key + offset per data block)";
                case FILE_INFO -> "FileInfo: " + b.getLines().size() + " key/value pairs";
                case GENERAL_BLOOM_META -> "Bloom metadata: hash count, key count, where the bit array is";
                default -> "";
            };
            out.printf("  %8d %7d  %-9s %s%n", offset, b.getOnDiskSizeWithHeader(), b.getBlockType().getMagic(),
                    content);
            offset += b.getOnDiskSizeWithHeader();
        }
        out.printf("  %8d %7d  %-9s %s%n", end, FixedFileTrailer.TRAILER_SIZE, BlockType.TRAILER.getMagic(),
                "fixed trailer: loadOnOpenDataOffset=" + t.getLoadOnOpenDataOffset() + ", " + t.getEntryCount()
                        + " cells, version 3.3 <- the reader starts here");
        out.println("  Every block = header (" + HFileBlock.HEADER_SIZE + " bytes: magic, onDiskSizeWithoutHeader,"
                + " prevBlockOffset, checksum) + body.");
    }

    private void printBlockHeaders(HFileReaderImpl r) throws IOException {
        out.println();
        out.println("Block headers:");
        long offset = 0;
        long end = r.length() - FixedFileTrailer.TRAILER_SIZE;
        while (offset < end) {
            HFileBlock b = HFileBlock.read(r.getInput(), offset, r.getName());
            out.printf("  @%-8d %s onDiskSizeWithoutHeader=%d prevBlockOffset=%d%n", offset,
                    b.getBlockType().getMagic(), b.getOnDiskSizeWithoutHeader(), b.getPrevBlockOffset());
            offset += b.getOnDiskSizeWithHeader();
        }
    }

    // --------------------------------------------------------------- meta
    private void printMeta(HFileReaderImpl r) {
        out.println();
        out.println("Trailer:");
        for (Map.Entry<String, String> e : r.getTrailer().toMap().entrySet()) {
            out.println("    " + e.getKey() + "=" + e.getValue());
        }
        out.println("Fileinfo:");
        for (Map.Entry<String, String> e : r.getHFileInfo().asMap().entrySet()) {
            out.println("    " + e.getKey() + " = " + e.getValue() + explainFileInfo(e.getKey()));
        }
        out.println("Mid-key: (not computed in EduBase - the original uses it to split regions)");
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

    private static String explainFileInfo(String key) {
        return switch (key) {
            case HFileInfo.MAX_SEQ_ID_KEY -> "   (all edits up to this sequence ID are in this file)";
            case HFileInfo.TIMERANGE_KEY -> "   (smallest..largest timestamp)";
            case HFileInfo.MAJOR_COMPACTION_KEY -> "   (true: no delete markers left)";
            default -> "";
        };
    }

    private void printBlockIndex(HFileReaderImpl r) {
        out.println();
        out.println("Block Index (root level, " + r.getDataBlockIndexReader().getRootBlockCount() + " entries):");
        out.printf("  %5s %8s %7s  %s%n", "block", "offset", "bytes", "first key");
        int i = 0;
        for (HFileBlockIndex.Entry e : r.getDataBlockIndexReader().getEntries()) {
            out.printf("  %5d %8d %7d  %s%n", i++, e.getOffset(), e.getOnDiskSize(),
                    KeyValueCodec.encodeKey(e.getFirstKey()));
        }
        out.println("  A Get for row R reads only the block whose first key is the last one <= R (binary search).");
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
    private void printStats(HFileReaderImpl r, List<Cell> cells) {
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
        out.println("    data blocks: " + r.getDataBlockIndexReader().getRootBlockCount() + ", file size: "
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
