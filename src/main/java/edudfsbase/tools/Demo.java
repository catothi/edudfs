package edudfsbase.tools;

import edudfsbase.HBaseConfiguration;
import edudfsbase.client.Connection;
import edudfsbase.client.ConnectionFactory;
import edudfsbase.fs.CommonFSUtils;
import edudfsbase.io.hfile.HFilePrettyPrinter;
import edudfsbase.regionserver.HRegion;
import edudfsbase.regionserver.HRegionServer;
import edudfsbase.regionserver.HStoreFile;
import edudfsbase.wal.WALPrettyPrinter;
import edudfs.common.Log;
import edudfs.tools.MiniDFSCluster;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Locale;

/**
 * Guided tour of EduDFSBase. Starts its own EduDFS (NameNode + 3 DataNodes, data in demo-data/) and
 * EduDFSBase (HMaster + 2 RegionServers) and runs through the core concepts as a shell session:
 * every "hbase&gt;" line can be typed in ./hbase shell just the same.
 *
 * With -Dedudfs.verbose=true the logs of all components are shown as well.
 */
public class Demo {

    private static HBaseShell shell;
    private static HBaseShell quietShell;

    public static void main(String[] args) throws Exception {
        Log.quiet = !Boolean.getBoolean("edudfs.verbose");
        File base = new File("demo-data");
        MiniDFSCluster.deleteRecursively(base);
        MiniDFSCluster dfs = new MiniDFSCluster(3, base);
        dfs.waitActive();
        MiniHBaseCluster hbase = new MiniHBaseCluster(2);
        hbase.waitForActiveAndReadyMaster(60_000);
        Connection conn = ConnectionFactory.createConnection();
        shell = new HBaseShell(conn, System.out);
        quietShell = new HBaseShell(conn, new PrintStream(OutputStream.nullOutputStream()));
        try {
            run(hbase);
        } finally {
            conn.close();
            hbase.close();
            dfs.close();
        }
    }

    private static void run(MiniHBaseCluster hbase) throws Exception {
        // ----------------------------------------------------------------------
        banner("1) Table, column families, regions",
                "A table has column FAMILIES (fixed) - the columns inside a family are free per row.",
                "SPLITS => ['m'] pre-splits the table into 2 regions: ['', 'm') and ['m', ''), one per RegionServer.");
        sh("create 'users', {NAME => 'info', VERSIONS => 3, BLOCKSIZE => 256}, {NAME => 'stats'}, SPLITS => ['m']");
        sh("list_regions 'users'");

        // ----------------------------------------------------------------------
        banner("2) Write path: WAL first, then MemStore",
                "Every put is appended to the WAL of the RegionServer (in HDFS, hflush) and then put into the",
                "MemStore. Nothing is in an HFile yet - SOURCE => true shows where each cell comes from.");
        sh("put 'users', 'alice', 'info:name', 'Alice Adams'");
        sh("put 'users', 'alice', 'info:email', 'alice@example.org'");
        sh("put 'users', 'bob', 'info:name', 'Bob Brown'");
        sh("put 'users', 'zoe', 'info:name', 'Zoe Zimmer'");
        sh("put 'users', 'zoe', 'stats:logins', '42'");
        sh("scan 'users', {SOURCE => true}");
        System.out.println("The WAL of the RegionServer that serves ['', 'm') - a text file in HDFS"
                + " (original: binary protobuf):");
        HRegionServer rsA = serverOf(hbase, "");
        new WALPrettyPrinter(System.out).run(new String[]{"--table", "-p",
                CommonFSUtils.getWALDirectoryName(rsA.getServerName())});

        // ----------------------------------------------------------------------
        banner("3) Flush: MemStore -> HFile",
                "flush writes each MemStore sorted into a new HFile (.tmp, then rename into the family directory).");
        for (int i = 1; i <= 24; i++) {
            shellQuiet(String.format("put 'users', 'user%02d', 'info:name', 'Name number %02d'", i, i));
            if (i % 3 == 0) {
                shellQuiet(String.format("put 'users', 'user%02d', 'info:city', 'Ingolstadt'", i));
            }
        }
        System.out.println("(24 more rows user01..user24 written)");
        sh("flush 'users'");
        String hfile = storeFiles(hbase, "m", "info").get(0).getPath();
        System.out.println("Inside the HFile (./hbase hfile --layout -b -f <path>): blocks, index, Bloom filter,"
                + " trailer");
        new HFilePrettyPrinter(System.out).run(new String[]{"--layout", "-b", "-f", hfile});
        System.out.println("\nThe same cells as a 'wide column' table (./hbase hfile --grid):");
        new HFilePrettyPrinter(System.out).run(new String[]{"--grid", "-f", hfile});

        // ----------------------------------------------------------------------
        banner("4) Read path: MemStore + HFiles, Bloom filter, block index, BlockCache",
                "A second flush creates a second HFile. A get then has to look into the MemStore and both files -",
                "unless the Bloom filter of a file says 'row definitely not here'.");
        sh("put 'users', 'user05', 'info:city', 'Munich'");
        sh("put 'users', 'user99', 'info:name', 'Late Comer'");
        sh("flush 'users'");
        sh("get 'users', 'user05', {TRACE => true, SOURCE => true}");
        sh("get 'users', 'user98', {TRACE => true}");
        sh("get 'users', 'user05', {TRACE => true}");

        // ----------------------------------------------------------------------
        banner("5) Versions and deletes: nothing is overwritten, nothing is removed right away",
                "A new value is a new cell with a newer timestamp; a delete writes a delete marker (tombstone).",
                "RAW => true shows everything that physically exists.");
        sh("put 'users', 'user07', 'info:name', 'Renamed 07'");
        sh("delete 'users', 'user05', 'info:city'");
        sh("deleteall 'users', 'user06'");
        sh("scan 'users', {STARTROW => 'user05', STOPROW => 'user08'}");
        sh("scan 'users', {STARTROW => 'user05', STOPROW => 'user08', RAW => true, VERSIONS => 10, SOURCE => true}");

        // ----------------------------------------------------------------------
        banner("6) Compaction",
                "After the third flush the store has " + HBaseConfiguration.COMPACTION_MIN_FILES + " HFiles"
                        + " (hbase.hstore.compactionThreshold)",
                "-> a minor compaction starts in the background.",
                "If it takes in ALL files, it may drop delete markers and the cells they hide; old versions beyond",
                "VERSIONS are dropped as well. major_compact always merges all files into one.");
        sh("flush 'users'");
        Thread.sleep(1500);
        sh("compaction_history");
        sh("major_compact 'users'");
        sh("scan 'users', {STARTROW => 'user05', STOPROW => 'user08', RAW => true, VERSIONS => 10, SOURCE => true}");

        // ----------------------------------------------------------------------
        HRegionServer victim = serverOf(hbase, "");
        int victimIndex = hbase.getRegionServer(0) == victim ? 0 : 1;
        banner("7) RegionServer crash: recovery from the WAL",
                "We write 3 rows (only in MemStore + WAL!) and then kill " + victim.getServerName() + ".",
                "The master notices the missing heartbeats after zookeeper.session.timeout = "
                        + HBaseConfiguration.SESSION_TIMEOUT / 1000 + " s",
                "and runs the ServerCrashProcedure:",
                "rename WALs/<server> to -splitting, recover the lease, split the WAL into recovered.edits,",
                "open the region on the other RegionServer, which replays the edits.");
        sh("put 'users', 'carol', 'info:name', 'Carol Crash'");
        sh("put 'users', 'dave', 'info:name', 'Dave Durable'");
        sh("put 'users', 'eve', 'info:name', 'Eve Everlasting'");
        hbase.killRegionServer(victimIndex);
        System.out.println(">>> " + victim.getServerName() + " killed (no flush, WAL left open in HDFS)");
        long start = System.currentTimeMillis();
        while (hbase.getMaster().getProcedures().isEmpty()
                || !hbase.getMaster().getProcedures().get(0).getState().equals("SUCCESS")) {
            Thread.sleep(250);
        }
        hbase.waitUntilAllRegionsAssigned(60_000);
        System.out.printf(Locale.ROOT, ">>> regions available again after %.1f s%n%n",
                (System.currentTimeMillis() - start) / 1000.0);
        sh("scan 'users', {STARTROW => 'a', STOPROW => 'f', SOURCE => true}");
        sh("list_regions 'users'");
        sh("metasave");
        banner("Done. Start your own cluster with ./hdfs minicluster and ./hbase minicluster, then ./hbase shell.");
    }

    // ---------------------------------------------------------------- helpers
    private static void sh(String cmd) {
        System.out.println("hbase> " + cmd);
        shell.execute(cmd);
    }

    private static void shellQuiet(String cmd) {
        quietShell.execute(cmd);
    }

    /** The RegionServer that serves the region of 'users' with this start key. */
    private static HRegionServer serverOf(MiniHBaseCluster hbase, String startKey) {
        for (int i = 0; i < hbase.getNumRegionServers(); i++) {
            HRegionServer rs = hbase.getRegionServer(i);
            for (HRegion r : rs.getOnlineRegions()) {
                if (new String(r.getRegionInfo().getStartKey()).equals(startKey)) {
                    return rs;
                }
            }
        }
        throw new IllegalStateException("No server for region with start key '" + startKey + "'");
    }

    private static List<HStoreFile> storeFiles(MiniHBaseCluster hbase, String startKey, String family) {
        for (HRegion r : serverOf(hbase, startKey).getOnlineRegions()) {
            if (new String(r.getRegionInfo().getStartKey()).equals(startKey)) {
                return r.getStore(family).getStorefiles();
            }
        }
        throw new IllegalStateException();
    }

    private static void banner(String title, String... lines) {
        System.out.println();
        System.out.println("#".repeat(100));
        System.out.println("# " + title);
        for (String l : lines) {
            System.out.println("#   " + l);
        }
        System.out.println("#".repeat(100));
        System.out.println();
    }
}
