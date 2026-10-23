package edudfsbase.tools;

import edudfsbase.TableName;
import edudfsbase.client.Admin;
import edudfsbase.client.ColumnFamilyDescriptorBuilder;
import edudfsbase.client.Connection;
import edudfsbase.client.ConnectionFactory;
import edudfsbase.client.Get;
import edudfsbase.client.Put;
import edudfsbase.client.ResultScanner;
import edudfsbase.client.Scan;
import edudfsbase.client.Table;
import edudfsbase.client.TableDescriptorBuilder;
import edudfsbase.fs.CommonFSUtils;
import edudfsbase.fs.HFileSystem;
import edudfsbase.regionserver.BloomType;
import edudfsbase.util.Bytes;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Corresponds to org.apache.hadoop.hbase.PerformanceEvaluation ({@code hbase pe}): generates load - here
 * mainly to get HFiles large enough to see a multi-level block index (B+ tree).
 * <pre>
 *   ./hbase pe [--rows=3000] [--table=TestTable] [--valueSize=32] [--columns=1] [--presplit=0]
 *              [--blockSize=1024] [--bloomFilter=ROW] [--flush] [--compact] &lt;command&gt; [1]
 *   commands: sequentialWrite, randomWrite, randomRead, scan
 * </pre>
 * Row keys: zero-padded numbers, 10 digits (original: 26). Family "info", qualifiers "0".."columns-1",
 * random values. The original runs nclients threads or MapReduce tasks; here there is one client.
 * EduDFSBase additions: --blockSize (family BLOCKSIZE of a new table), --flush (flush at the end) and
 * --compact (flush + major_compact: ONE HFile per region with the whole index), and the HFiles are listed
 * at the end with the command to look inside.
 */
public class PerformanceEvaluation {

    private int rows = 3000;
    private String table = "TestTable";
    private int valueSize = 32;
    private int columns = 1;
    private int presplit;
    private int blockSize = ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE;
    private BloomType bloom = BloomType.ROW;
    private boolean flush;
    private boolean compact;

    static final byte[] FAMILY = Bytes.toBytes("info");

    public static void main(String[] args) throws Exception {
        Log.quiet = !Boolean.getBoolean("edudfs.verbose");
        System.exit(new PerformanceEvaluation().run(args));
    }

    static String format(long i) {
        return String.format("%010d", i);
    }

    int run(String[] args) throws IOException {
        String command = null;
        for (String a : args) {
            if (a.startsWith("--rows=")) {
                rows = Integer.parseInt(value(a));
            } else if (a.startsWith("--table=")) {
                table = value(a);
            } else if (a.startsWith("--valueSize=")) {
                valueSize = Integer.parseInt(value(a));
            } else if (a.startsWith("--columns=")) {
                columns = Integer.parseInt(value(a));
            } else if (a.startsWith("--presplit=")) {
                presplit = Integer.parseInt(value(a));
            } else if (a.startsWith("--blockSize=")) {
                blockSize = Integer.parseInt(value(a));
            } else if (a.startsWith("--bloomFilter=")) {
                bloom = BloomType.valueOf(value(a).toUpperCase(Locale.ROOT));
            } else if (a.equals("--flush")) {
                flush = true;
            } else if (a.equals("--compact")) {
                compact = true;
            } else if (a.equals("--nomapred")) {
                continue;  // accepted for compatibility: there is no MapReduce here anyway
            } else if (a.startsWith("--")) {
                return usage("Unknown option " + a);
            } else if (command == null) {
                command = a;
            } else if (!a.equals("1")) {
                return usage("Only one client (nclients = 1) in EduDFSBase");
            }
        }
        if (command == null) {
            return usage(null);
        }
        try (Connection conn = ConnectionFactory.createConnection()) {
            conn.checkMasterReachable();
            TableName tn = TableName.valueOf(table);
            Admin admin = conn.getAdmin();
            long start = System.nanoTime();
            switch (command) {
                case "sequentialWrite", "randomWrite" -> {
                    createTableIfMissing(admin, tn);
                    write(conn.getTable(tn), command.equals("randomWrite"));
                }
                case "randomRead" -> randomRead(conn.getTable(tn));
                case "scan" -> scan(conn.getTable(tn));
                default -> {
                    return usage("Unknown command " + command);
                }
            }
            System.out.printf(Locale.ROOT, "Finished %s in %d ms over %d rows%n", command,
                    (System.nanoTime() - start) / 1_000_000, rows);
            if (command.endsWith("Write")) {
                if (flush || compact) {
                    System.out.print(admin.flush(tn));
                }
                if (compact) {
                    admin.majorCompact(tn);
                    System.out.println("major_compact '" + table + "' done - one HFile per region.");
                }
                listHFiles(tn);
            }
        }
        return 0;
    }

    private static String value(String a) {
        return a.substring(a.indexOf('=') + 1);
    }

    private int usage(String error) {
        if (error != null) {
            System.err.println("ERROR: " + error);
        }
        System.err.println("""
                Usage: hbase pe [options] <command> [1]
                Options:
                 --rows=N          rows to write/read (default 3000; original: 1048576)
                 --table=NAME      table (default TestTable)
                 --valueSize=N     bytes per value (default 32; original: 1024)
                 --columns=N       columns per row, qualifiers 0..N-1 (default 1)
                 --presplit=N      create the table with N regions (default 1)
                 --bloomFilter=T   NONE, ROW, ROWCOL (default ROW)
                 --blockSize=N     BLOCKSIZE of the family when the table is created (default 1024; EduDFSBase)
                 --flush           flush at the end (EduDFSBase)
                 --compact         flush + major_compact at the end: one HFile per region (EduDFSBase)
                Commands:
                 sequentialWrite   rows 0000000000 .. N-1 in order
                 randomWrite       N rows with random keys in [0, N)
                 randomRead        N gets with random keys
                 scan              scan the whole table
                Example: ./hbase pe --rows=3000 --compact sequentialWrite 1
                         then ./hbase hfile -m -f <path printed at the end>  (or --tree, --html out.html)""");
        return 1;
    }

    private void createTableIfMissing(Admin admin, TableName tn) throws IOException {
        if (admin.tableExists(tn)) {
            System.out.println("Table " + table + " exists - writing into it (BLOCKSIZE etc. stay as they are).");
            return;
        }
        byte[][] splits = new byte[Math.max(0, presplit - 1)][];
        for (int i = 1; i < presplit; i++) {
            splits[i - 1] = Bytes.toBytes(format((long) rows * i / presplit));
        }
        admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(ColumnFamilyDescriptorBuilder
                .newBuilder(FAMILY).setBlocksize(blockSize).setBloomFilterType(bloom).build()).build(), splits);
        System.out.println("Created table " + table + " {NAME => 'info', BLOCKSIZE => '" + blockSize
                + "', BLOOMFILTER => '" + bloom + "'}" + (presplit > 1 ? ", " + presplit + " regions" : ""));
    }

    private void write(Table t, boolean random) throws IOException {
        Random rnd = new Random();
        List<Put> batch = new ArrayList<>();
        byte[] value = new byte[valueSize];
        for (int i = 0; i < rows; i++) {
            long key = random ? rnd.nextInt(rows) : i;
            Put p = new Put(Bytes.toBytes(format(key)));
            for (int c = 0; c < columns; c++) {
                for (int b = 0; b < valueSize; b++) {
                    value[b] = (byte) ('a' + rnd.nextInt(26));
                }
                p.addColumn(FAMILY, Bytes.toBytes(String.valueOf(c)), value.clone());
            }
            batch.add(p);
            if (batch.size() == 100) {
                t.put(batch);
                batch.clear();
            }
            if ((i + 1) % 1000 == 0) {
                System.out.println("  " + (i + 1) + " rows written");
            }
        }
        if (!batch.isEmpty()) {
            t.put(batch);
        }
    }

    private void randomRead(Table t) throws IOException {
        Random rnd = new Random();
        int found = 0;
        for (int i = 0; i < rows; i++) {
            if (!t.get(new Get(Bytes.toBytes(format(rnd.nextInt(rows))))).isEmpty()) {
                found++;
            }
        }
        System.out.println("  " + found + " of " + rows + " gets found a row");
    }

    private void scan(Table t) throws IOException {
        int n = 0;
        try (ResultScanner s = t.getScanner(new Scan())) {
            while (s.next() != null) {
                n++;
            }
        }
        System.out.println("  scanned " + n + " rows");
    }

    /** The HFiles of the table in HDFS, with the command to look inside. */
    private static void listHFiles(TableName tn) throws IOException {
        try (HFileSystem fs = new HFileSystem()) {
            String tableDir = CommonFSUtils.getTableDir(tn);
            List<String> files = new ArrayList<>();
            for (HdfsFileStatus region : fs.listStatus(tableDir)) {
                String family = region.getPath() + "/info";
                if (region.isDir() && fs.exists(family)) {
                    for (HdfsFileStatus f : fs.listStatus(family)) {
                        if (!f.isDir()) {
                            files.add(f.getPath() + "  (" + f.getLen() + " bytes)");
                        }
                    }
                }
            }
            System.out.println(files.isEmpty() ? "No HFiles yet - the data is still in the MemStore (flush '"
                    + tn + "' or --flush)." : "HFiles of " + tn + ":");
            files.forEach(f -> System.out.println("  " + f));
            if (!files.isEmpty()) {
                String p = files.get(0).substring(0, files.get(0).indexOf("  ("));
                System.out.println("Look inside: ./hbase hfile -m -f " + p);
                System.out.println("             ./hbase hfile --tree -f " + p);
                System.out.println("             ./hbase hfile --html hfile.html -f " + p + "   (open hfile.html in a browser)");
            }
        }
    }
}
