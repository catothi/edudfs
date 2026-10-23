package edudfsbase.wal;

import edudfsbase.Cell;
import edudfsbase.fs.CommonFSUtils;
import edudfsbase.fs.HFileSystem;
import edudfsbase.util.Bytes;
import edudfs.client.DFSInputStream;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hbase.wal.WALPrettyPrinter ({@code hbase wal}): shows the entries of
 * WAL files - also of recovered.edits and oldWALs, since they have the same format.
 * <pre>
 *   ./hbase wal [-p] [-r encodedRegion] [-s sequence] [-w row] [--table] &lt;file or directory&gt;...
 *     -p        print values                     (original)
 *     -r        only this region (encoded name, a prefix is enough)
 *     -s        only this sequence ID
 *     -w        only this row
 *     --table   one line per cell (EduDFSBase)
 * </pre>
 * Paths in HDFS; "file:" for a local copy. A directory: all files in it in name order (= time order).
 */
public class WALPrettyPrinter {

    private final PrintStream out;
    private boolean printValues;
    private boolean table;
    private String region;
    private long sequence = -1;
    private byte[] row;

    public WALPrettyPrinter(PrintStream out) {
        this.out = out;
    }

    public static void main(String[] args) throws Exception {
        Log.quiet = !Boolean.getBoolean("edudfs.verbose");
        System.exit(new WALPrettyPrinter(System.out).run(args));
    }

    public int run(String[] args) throws IOException {
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p" -> printValues = true;
                case "--table" -> table = true;
                case "-r" -> region = args[++i];
                case "-s" -> sequence = Long.parseLong(args[++i]);
                case "-w" -> row = Bytes.toBytesBinary(args[++i]);
                default -> paths.add(args[i]);
            }
        }
        if (paths.isEmpty()) {
            out.println("Usage: hbase wal [-p] [-r encodedRegion] [-s sequence] [-w row] [--table] <file or dir>...");
            return 1;
        }
        HFileSystem fs = null;
        try {
            for (String p : paths) {
                if (p.startsWith("file:")) {
                    try (InputStream in = new FileInputStream(p.substring(5))) {
                        print(p, in);
                    }
                    continue;
                }
                if (fs == null) {
                    fs = new HFileSystem();
                }
                HdfsFileStatus st = fs.getFileStatus(p);
                List<String> files = new ArrayList<>();
                if (st.isDir()) {
                    for (HdfsFileStatus f : fs.listStatus(p)) {
                        if (!f.isDir()) {
                            files.add(f.getPath());
                        }
                    }
                    files.sort(null);
                } else {
                    files.add(p);
                }
                for (String f : files) {
                    try (DFSInputStream in = fs.openStream(f)) {
                        boolean open = fs.getFileStatus(f).isUnderConstruction();
                        print(f + (open ? "  (open, still being written)" : ""), in);
                    }
                }
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        return 0;
    }

    private void print(String name, InputStream in) throws IOException {
        out.println("Reading " + name);
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        int entries = 0;
        int cells = 0;
        Map<String, Integer> perRegion = new LinkedHashMap<>();
        long minSeq = Long.MAX_VALUE;
        long maxSeq = Long.MIN_VALUE;
        TextLogReader reader = new TextLogReader(in, name);
        if (table) {
            out.printf("  %6s  %-8s  %-14s %-16s %-14s %-12s %s%n", "seqid", "region", "row", "family:qualifier",
                    "timestamp", "type", printValues ? "value" : "");
        }
        WAL.Entry e;
        while ((e = reader.next()) != null) {
            WALKeyImpl k = e.getKey();
            if ((region != null && !k.getEncodedRegionName().startsWith(region))
                    || (sequence >= 0 && k.getSequenceId() != sequence)) {
                continue;
            }
            List<Cell> selected = new ArrayList<>();
            for (Cell c : e.getEdit().getCells()) {
                if (row == null || Bytes.equals(row, c.getRow())) {
                    selected.add(c);
                }
            }
            if (selected.isEmpty()) {
                continue;
            }
            entries++;
            cells += selected.size();
            perRegion.merge(k.getEncodedRegionName().substring(0, 8), 1, Integer::sum);
            minSeq = Math.min(minSeq, k.getSequenceId());
            maxSeq = Math.max(maxSeq, k.getSequenceId());
            if (table) {
                for (Cell c : selected) {
                    out.printf("  %6d  %-8s  %-14s %-16s %-14d %-12s %s%n", k.getSequenceId(),
                            k.getEncodedRegionName().substring(0, 8), Bytes.toStringBinary(c.getRow()),
                            Bytes.toStringBinary(c.getFamily()) + ":" + Bytes.toStringBinary(c.getQualifier()),
                            c.getTimestamp(), c.getType(), printValues ? Bytes.toStringBinary(c.getValue()) : "");
                }
                continue;
            }
            // format of the original WALPrettyPrinter
            out.println("Sequence=" + k.getSequenceId() + " , table=" + k.getTableName() + " , region="
                    + k.getEncodedRegionName() + " at write timestamp=" + fmt.format(new Date(k.getWriteTime())));
            for (Cell c : selected) {
                out.println("row=" + Bytes.toStringBinary(c.getRow()) + ", type=" + c.getType());
                out.println("    column=" + Bytes.toStringBinary(c.getFamily()) + ":"
                        + Bytes.toStringBinary(c.getQualifier()));
                out.println("    timestamp=" + c.getTimestamp());
                if (printValues) {
                    out.println("    value: " + Bytes.toStringBinary(c.getValue()));
                }
            }
        }
        out.println("  -> " + entries + " entries, " + cells + " cells"
                + (entries > 0 ? ", sequence IDs " + minSeq + ".." + maxSeq + ", entries per region " + perRegion : "")
                + (reader.hasTrailer() ? ", closed properly (LAWP trailer)" : ", no trailer (open or crashed writer)")
                + (reader.isTruncated() ? ", LAST ENTRY CUT OFF" : ""));
        out.println("  (file " + CommonFSUtils.getName(name.split(" ")[0]) + ")");
    }
}
