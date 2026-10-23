package edudfsbase.tools;

import edudfsbase.Cell;
import edudfsbase.KeyValue;
import edudfsbase.TableName;
import edudfsbase.client.Admin;
import edudfsbase.client.ColumnFamilyDescriptorBuilder;
import edudfsbase.client.Connection;
import edudfsbase.client.ConnectionFactory;
import edudfsbase.client.Delete;
import edudfsbase.client.Get;
import edudfsbase.client.Put;
import edudfsbase.client.Result;
import edudfsbase.client.ResultScanner;
import edudfsbase.client.Scan;
import edudfsbase.client.Table;
import edudfsbase.client.TableDescriptorBuilder;
import edudfsbase.regionserver.HRegion;
import edudfsbase.regionserver.HRegionServer;
import edudfsbase.regionserver.HStore;
import edudfsbase.regionserver.compactions.CompactionRecord;
import edudfsbase.util.Bytes;
import edudfs.tools.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end tests: EduDFS (NameNode + 3 DataNodes) + HMaster + 2 RegionServers in one JVM.
 * Shortened timings come from the surefire configuration in pom.xml.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class MiniHBaseClusterTest {

    private static final TableName T = TableName.valueOf("t");
    private static final byte[] CF = Bytes.toBytes("cf");

    @TempDir
    File baseDir;

    private MiniDFSCluster dfs;
    private MiniHBaseCluster hbase;
    private Connection conn;
    private Admin admin;

    @BeforeEach
    void start() throws Exception {
        dfs = new MiniDFSCluster(3, baseDir);
        dfs.waitActive();
        hbase = new MiniHBaseCluster(2);
        hbase.waitForActiveAndReadyMaster(30_000);
        conn = ConnectionFactory.createConnection();
        admin = conn.getAdmin();
    }

    @AfterEach
    void stop() {
        conn.close();
        hbase.close();
        dfs.close();
    }

    // ------------------------------------------------------------ helpers
    interface Condition {
        boolean holds() throws Exception;
    }

    private static void waitFor(String what, long timeoutMs, Condition c) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!c.holds()) {
            if (System.currentTimeMillis() > deadline) {
                fail("Timeout waiting for: " + what);
            }
            Thread.sleep(100);
        }
    }

    private Table createTable(int versions, byte[]... splits) throws Exception {
        admin.createTable(TableDescriptorBuilder.newBuilder(T)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).setMaxVersions(versions).build())
                .build(), splits.length == 0 ? null : splits);
        hbase.waitUntilAllRegionsAssigned(30_000);
        return conn.getTable(T);
    }

    private static Put put(String row, String q, String v) {
        return new Put(Bytes.toBytes(row)).addColumn(CF, Bytes.toBytes(q), Bytes.toBytes(v));
    }

    private static String value(Result r, String q) {
        byte[] v = r.getValue(CF, Bytes.toBytes(q));
        return v == null ? null : Bytes.toString(v);
    }

    private List<HRegion> allRegions() {
        List<HRegion> r = new ArrayList<>();
        for (int i = 0; i < hbase.getNumRegionServers(); i++) {
            HRegionServer rs = hbase.getRegionServer(i);
            if (!rs.isStopped()) {
                r.addAll(rs.getOnlineRegions());
            }
        }
        return r;
    }

    private HStore store() {
        return allRegions().get(0).getStore("cf");
    }

    // =============================================================== tests
    @Test
    void putGetScanDeleteAcrossPreSplitRegions() throws Exception {
        Table t = createTable(1, Bytes.toBytes("g"), Bytes.toBytes("n"));
        assertEquals(3, admin.getRegions(T).size());
        String[] rows = {"apple", "grape", "kiwi", "mango", "orange", "zucchini"};
        for (String r : rows) {
            t.put(put(r, "color", "c-" + r));
        }
        assertEquals("c-kiwi", value(t.get(new Get(Bytes.toBytes("kiwi"))), "color"));
        assertTrue(t.get(new Get(Bytes.toBytes("banana"))).isEmpty());
        List<String> scanned = new ArrayList<>();
        try (ResultScanner s = t.getScanner(new Scan().setCaching(2))) {
            for (Result r : s) {
                scanned.add(Bytes.toString(r.getRow()));
            }
        }
        assertEquals(List.of(rows), scanned);
        t.delete(new Delete(Bytes.toBytes("mango")));
        assertTrue(t.get(new Get(Bytes.toBytes("mango"))).isEmpty());
        List<String> range = new ArrayList<>();
        Scan hToP = new Scan().withStartRow(Bytes.toBytes("h")).withStopRow(Bytes.toBytes("p"));
        try (ResultScanner s = t.getScanner(hToP)) {
            for (Result r : s) {
                range.add(Bytes.toString(r.getRow()));
            }
        }
        assertEquals(List.of("kiwi", "orange"), range);
    }

    @Test
    void versionsAndDeleteMarkersInMemStoreAndHFiles() throws Exception {
        Table t = createTable(3);
        for (int i = 1; i <= 4; i++) {
            t.put(new Put(Bytes.toBytes("r")).addColumn(CF, Bytes.toBytes("a"), i * 10L, Bytes.toBytes("v" + i)));
        }
        admin.flush(T);
        Result r = t.get(new Get(Bytes.toBytes("r")).readVersions(10));
        assertEquals(3, r.size());  // VERSIONS => 3
        assertEquals("v4", value(r, "a"));
        t.delete(new Delete(Bytes.toBytes("r")).addColumns(CF, Bytes.toBytes("a"), 30));
        r = t.get(new Get(Bytes.toBytes("r")).readVersions(10));
        assertEquals(1, r.size());
        assertEquals("v4", value(r, "a"));
        Result raw;
        try (ResultScanner s = t.getScanner(new Scan().setRaw(true).readAllVersions())) {
            raw = s.next();
        }
        // sorted by timestamp descending: a/40/Put, a/30/DeleteColumn (before the Put with the same ts), a/30/Put, ...
        assertEquals(KeyValue.Type.Put, raw.rawCells()[0].getType());
        assertEquals(KeyValue.Type.DeleteColumn, raw.rawCells()[1].getType());
        assertEquals(30, raw.rawCells()[1].getTimestamp());
        assertEquals(5, raw.size());  // marker + 4 versions (one more than VERSIONS: not yet compacted)
    }

    @Test
    void flushWritesHFileAndGetUsesBloomFilter() throws Exception {
        Table t = createTable(1);
        for (int i = 0; i < 50; i++) {
            t.put(put(String.format("row%02d", i), "a", "value" + i));
        }
        String report = admin.flush(T);
        assertTrue(report.contains("50 cells"), report);
        assertEquals(1, store().getStorefiles().size());
        assertEquals(0, store().getMemStoreSize());
        t.put(put("row00a", "a", "x"));  // second HFile: key range row00a..row99 covers row07,
        t.put(put("row99", "a", "y"));   // but row07 is not in it -> the Bloom filter must say NO
        admin.flush(T);
        Result r = t.get(new Get(Bytes.toBytes("row07")).setTrace(true).setShowSource(true));
        assertEquals("value7", value(r, "a"));
        assertTrue(r.getSources().get(0).startsWith("HFile "), r.getSources().toString());
        String trace = String.join("\n", r.getTrace());
        assertTrue(trace.contains("Bloom filter (ROW) says NO"), trace);
        assertTrue(trace.contains("Bloom filter (ROW) says MAYBE"), trace);
    }

    @Test
    void compactionsDropVersionsAndDeletes() throws Exception {
        Table t = createTable(1);
        t.put(put("a", "q", "old"));
        admin.flush(T);
        t.put(put("a", "q", "new"));
        t.put(put("b", "q", "gone"));
        admin.flush(T);
        t.delete(new Delete(Bytes.toBytes("b")));
        admin.flush(T);
        // 3 HFiles = compactionThreshold: a minor compaction may run in the background - wait for it
        waitFor("background compaction", 10_000, () -> store().getStorefiles().size() <= 3);
        List<CompactionRecord> recs = admin.majorCompact(T);
        assertEquals(1, store().getStorefiles().size());
        long out = recs.isEmpty() ? store().getStorefiles().get(0).getReader().getEntries()
                : recs.get(0).getOutput().entries();
        assertEquals(1, out);  // only a/q=new survives
        assertEquals("new", value(t.get(new Get(Bytes.toBytes("a"))), "q"));
        assertTrue(t.get(new Get(Bytes.toBytes("b"))).isEmpty());
    }

    @Test
    void regionServerCrashIsRecoveredFromWAL() throws Exception {
        Table t = createTable(1, Bytes.toBytes("m"));
        for (int i = 0; i < 20; i++) {
            t.put(put("a" + i, "q", "v" + i));
            t.put(put("z" + i, "q", "v" + i));
        }
        int victim = hbase.getRegionServer(0).getOnlineRegions().isEmpty() ? 1 : 0;
        assertFalse(hbase.getRegionServer(victim).getOnlineRegions().isEmpty());
        hbase.killRegionServer(victim);  // MemStore lost, WAL still open in HDFS
        waitFor("ServerCrashProcedure finished", 40_000, () -> !hbase.getMaster().getProcedures().isEmpty()
                && "SUCCESS".equals(hbase.getMaster().getProcedures().get(0).getState()));
        hbase.waitUntilAllRegionsAssigned(30_000);
        for (int i = 0; i < 20; i++) {
            assertEquals("v" + i, value(t.get(new Get(Bytes.toBytes("a" + i))), "q"));
            assertEquals("v" + i, value(t.get(new Get(Bytes.toBytes("z" + i))), "q"));
        }
        assertEquals(2, hbase.getRegionServer(1 - victim).getOnlineRegions().size());
    }

    @Test
    void cleanRestartOfHBaseKeepsData() throws Exception {
        Table t = createTable(1);
        t.put(put("k", "q", "persisted"));
        hbase.close();  // flushes on close
        hbase = new MiniHBaseCluster(1);
        hbase.waitUntilAllRegionsAssigned(30_000);
        assertEquals("persisted", value(conn.getTable(T).get(new Get(Bytes.toBytes("k"))), "q"));
    }

    @Test
    void hbaseRestartAfterCrashReplaysWAL() throws Exception {
        Table t = createTable(1);
        t.put(put("k", "q", "only-in-wal"));
        for (int i = 0; i < 2; i++) {
            hbase.killRegionServer(i);
        }
        hbase.stopMaster();
        hbase = new MiniHBaseCluster(1);  // new master finds the WAL dirs of the dead servers
        hbase.waitUntilAllRegionsAssigned(30_000);
        Result r = conn.getTable(T).get(new Get(Bytes.toBytes("k")));
        assertEquals("only-in-wal", value(r, "q"));
        assertNull(value(r, "other"));
        Cell c = r.rawCells()[0];
        assertArrayEquals(Bytes.toBytes("k"), c.getRow());
    }
}
