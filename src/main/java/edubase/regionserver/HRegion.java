package edubase.regionserver;

import edubase.Cell;
import edubase.CellComparator;
import edubase.HBaseConfiguration;
import edubase.HConstants;
import edubase.KeyValue;
import edubase.NoSuchColumnFamilyException;
import edubase.NotServingRegionException;
import edubase.RegionInfo;
import edubase.client.ColumnFamilyDescriptor;
import edubase.client.Delete;
import edubase.client.Get;
import edubase.client.Mutation;
import edubase.client.Result;
import edubase.client.Scan;
import edubase.client.TableDescriptor;
import edubase.fs.HFileSystem;
import edubase.io.hfile.LruBlockCache;
import edubase.regionserver.compactions.CompactionRecord;
import edubase.util.Bytes;
import edubase.util.EnvironmentEdgeManager;
import edubase.util.ReadTrace;
import edubase.wal.TextLogReader;
import edubase.wal.WAL;
import edubase.wal.WALEdit;
import edubase.wal.WALKeyImpl;
import edudfs.client.DFSInputStream;
import edudfs.common.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.HRegion: a region = the key range
 * [startKey, endKey) of a table, with one {@link HStore} per column family.
 *
 * <b>Write path</b> ({@link #batchMutate}, original: doMiniBatchMutate):
 * <ol>
 *   <li>set timestamps ("now" for LATEST_TIMESTAMP), resolve deletes,</li>
 *   <li>MVCC begin -&gt; sequence ID,</li>
 *   <li>append the edit to the WAL and sync (hflush) - from now on it survives a crash,</li>
 *   <li>cells into the MemStores of the stores,</li>
 *   <li>MVCC complete -&gt; readers see the row,</li>
 *   <li>MemStore larger than hbase.hregion.memstore.flush.size? -&gt; request a flush.</li>
 * </ol>
 * <b>Read path</b>: RegionScanner -&gt; one StoreScanner per family -&gt; KeyValueHeap over MemStore + HFiles.
 *
 * Simplification: the original locks per row (row locks) and lets writes on different rows run in
 * parallel, with one WAL sync for many edits. Here the writes of a region are serialized.
 */
public class HRegion {

    private static final String LOG = "HRegion";

    private final RegionInfo regionInfo;
    private final TableDescriptor htd;
    private final HRegionFileSystem fs;
    private final WAL wal;
    private final HRegionServer rsServices;
    private final Map<String, HStore> stores = new TreeMap<>();
    private final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(0);
    /** Writes and the MemStore snapshot of a flush exclude each other. */
    private final Object updatesLock = new Object();
    private final Object flushLock = new Object();
    private volatile boolean closing;
    private volatile boolean closed;
    private long openSeqNum;
    private long replayedEdits;
    private volatile long lastFlushTime;
    private volatile long lastFlushSeqId;
    private final AtomicLong readRequestsCount = new AtomicLong();
    private final AtomicLong writeRequestsCount = new AtomicLong();

    private HRegion(RegionInfo regionInfo, TableDescriptor htd, HRegionFileSystem fs, WAL wal, HRegionServer rs) {
        this.regionInfo = regionInfo;
        this.htd = htd;
        this.fs = fs;
        this.wal = wal;
        this.rsServices = rs;
    }

    /** Creates the region in HDFS (directory + .regioninfo). Called by the master on create table. */
    public static void createRegionDir(HFileSystem fs, RegionInfo info) throws IOException {
        HRegionFileSystem.createRegionOnFileSystem(fs, info);
    }

    /** Opens a region: open the stores (HFiles), replay recovered.edits, set the sequence ID. */
    public static HRegion openHRegion(RegionInfo info, TableDescriptor htd, HFileSystem fs, WAL wal,
                                      LruBlockCache cache, HRegionServer rs) throws IOException {
        HRegion r = new HRegion(info, htd, HRegionFileSystem.openRegionFromFileSystem(fs, info), wal, rs);
        r.initialize(cache);
        return r;
    }

    private void initialize(LruBlockCache cache) throws IOException {
        Map<String, Long> maxSeqIdInStores = new TreeMap<>();
        long maxSeqId = 0;
        for (ColumnFamilyDescriptor f : htd.getColumnFamilies()) {
            HStore s = new HStore(this, f, fs, cache);
            stores.put(f.getNameAsString(), s);
            maxSeqIdInStores.put(f.getNameAsString(), s.getMaxSequenceId());
            maxSeqId = Math.max(maxSeqId, s.getMaxSequenceId());
        }
        mvcc.advanceTo(maxSeqId);
        long replayedMax = replayRecoveredEditsIfAny(maxSeqIdInStores);
        mvcc.advanceTo(Math.max(maxSeqId, replayedMax));
        openSeqNum = mvcc.getWritePoint() + 1;
        for (HStore s : stores.values()) {
            lastFlushSeqId = Math.max(lastFlushSeqId, s.getMaxSequenceId());  // includes the flush after a replay
        }
        lastFlushTime = EnvironmentEdgeManager.currentTime();
        Log.info(LOG, "Opened " + regionInfo.getRegionNameAsString() + "; next sequenceid=" + openSeqNum
                + (replayedEdits > 0 ? ", replayed " + replayedEdits + " edits from recovered.edits" : ""));
    }

    /**
     * Corresponds to HRegion.replayRecoveredEditsIfAny: the edits that the WALSplitter extracted from
     * the WAL of a crashed server. Only edits NEWER than the store's HFiles are replayed (the others
     * were flushed before the crash). Afterwards: flush and delete the recovered.edits.
     */
    private long replayRecoveredEditsIfAny(Map<String, Long> maxSeqIdInStores) throws IOException {
        List<String> files = fs.getRecoveredEditsFiles();
        if (files.isEmpty()) {
            return 0;
        }
        long maxSeen = 0;
        long skipped = 0;
        for (String file : files) {
            try (DFSInputStream in = fs.getFileSystem().openStream(file);
                 TextLogReader reader = new TextLogReader(in, file)) {
                WAL.Entry e;
                while ((e = reader.next()) != null) {
                    long seq = e.getKey().getSequenceId();
                    for (Cell c : e.getEdit().getCells()) {
                        String fam = Bytes.toString(c.getFamily());
                        HStore s = stores.get(fam);
                        if (s == null || seq <= maxSeqIdInStores.get(fam)) {
                            skipped++;
                            continue;  // already in an HFile (or family gone)
                        }
                        ((KeyValue) c).setSequenceId(seq);
                        s.add(c);
                        replayedEdits++;
                    }
                    maxSeen = Math.max(maxSeen, seq);
                }
            }
        }
        Log.info(LOG, regionInfo.getShortName() + ": replayed " + replayedEdits + " cells from "
                + files.size() + " recovered.edits file(s), skipped " + skipped + " already flushed cells");
        if (replayedEdits > 0) {
            mvcc.advanceTo(maxSeen);
            internalFlushcache("after replaying recovered.edits");
        }
        for (String file : files) {
            fs.getFileSystem().delete(file, false);
        }
        return maxSeen;
    }

    // ================================================================ write
    public void put(Mutation m) throws IOException {
        batchMutate(m);
    }

    public void delete(Delete d) throws IOException {
        batchMutate(d);
    }

    public void batchMutate(Mutation m) throws IOException {
        checkRow(m.getRow());
        long seq;
        synchronized (updatesLock) {
            checkOpen();
            long now = EnvironmentEdgeManager.currentTime();
            List<KeyValue> cells = prepareCells(m, now);
            if (cells.isEmpty()) {
                return;
            }
            MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
            seq = we.getWriteNumber();
            for (KeyValue kv : cells) {
                kv.setSequenceId(seq);
            }
            // 1. WAL first ...
            wal.appendData(regionInfo, new WALKeyImpl(regionInfo.getEncodedName(), regionInfo.getTable(), seq, now),
                    new WALEdit(cells));
            wal.sync();
            // 2. ... then the MemStore
            for (KeyValue kv : cells) {
                stores.get(Bytes.toString(kv.getFamily())).add(kv);
            }
            mvcc.completeAndWait(we);
        }
        writeRequestsCount.incrementAndGet();
        if (rsServices != null && getMemStoreDataSize() >= HBaseConfiguration.MEMSTORE_FLUSH_SIZE) {
            rsServices.requestFlush(this, "MemStore size " + getMemStoreDataSize() + " >= "
                    + "hbase.hregion.memstore.flush.size=" + HBaseConfiguration.MEMSTORE_FLUSH_SIZE);
        }
    }

    /** Timestamps "now", deletes without family -&gt; DeleteFamily for every family, Delete(latest) -&gt; real ts. */
    private List<KeyValue> prepareCells(Mutation m, long now) throws IOException {
        List<KeyValue> in = new ArrayList<>(m.getCells());
        if (m instanceof Delete && in.isEmpty()) {
            for (ColumnFamilyDescriptor f : htd.getColumnFamilies()) {
                in.add(new KeyValue(m.getRow(), f.getName(), null, m.getTimestamp(), KeyValue.Type.DeleteFamily, null));
            }
        }
        List<KeyValue> out = new ArrayList<>(in.size());
        for (KeyValue kv : in) {
            if (!htd.hasColumnFamily(kv.getFamily())) {
                throw new NoSuchColumnFamilyException("Column family " + Bytes.toStringBinary(kv.getFamily())
                        + " does not exist in region " + regionInfo.getRegionNameAsString() + " in table " + htd);
            }
            long ts = kv.getTimestamp();
            if (kv.getType() == KeyValue.Type.Delete && ts == HConstants.LATEST_TIMESTAMP) {
                // "delete the newest version": look up its timestamp (original: prepareDeleteTimestamps)
                ts = latestTimestamp(kv, now);
            } else if (ts == HConstants.LATEST_TIMESTAMP) {
                ts = now;
            }
            out.add(kv.withTimestamp(ts));
        }
        return out;
    }

    private long latestTimestamp(KeyValue kv, long now) throws IOException {
        Get g = new Get(kv.getRow()).addColumn(kv.getFamily(), kv.getQualifier());
        Result r = get(g);
        return r.isEmpty() ? now : r.rawCells()[0].getTimestamp();
    }

    // ================================================================= read
    public Result get(Get get) throws IOException {
        checkRow(get.getRow());
        Scan scan = new Scan(get);
        try (RegionScanner scanner = getScanner(scan)) {
            List<Cell> cells = new ArrayList<>();
            List<String> sources = scan.isShowSource() ? new ArrayList<>() : null;
            scanner.next(cells, sources);
            return new Result(cells, sources, scanner.getTrace() == null ? null : scanner.getTrace().getLines());
        }
    }

    public RegionScanner getScanner(Scan scan) throws IOException {
        checkOpen();
        readRequestsCount.incrementAndGet();
        ReadTrace trace = scan.isTrace() ? new ReadTrace() : null;
        long readPt = mvcc.getReadPoint();
        if (trace != null) {
            trace.add("region " + regionInfo.getRegionNameAsString() + ", MVCC read point " + readPt);
        }
        List<StoreScanner> scanners = new ArrayList<>();
        Collection<HStore> selected = new ArrayList<>();
        if (scan.hasFamilies()) {
            for (String fam : scan.getFamilyMap().keySet()) {
                HStore s = stores.get(fam);
                if (s == null) {
                    throw new NoSuchColumnFamilyException("Column family " + fam + " does not exist in table " + htd);
                }
                selected.add(s);
            }
        } else {
            selected = stores.values();
        }
        for (HStore s : selected) {
            NavigableSet<String> columns = scan.getFamilyMap().get(s.getColumnFamilyName());
            scanners.add(s.getScanner(scan, columns, readPt, trace));
        }
        return new RegionScanner(this, scan, scanners, trace);
    }

    // ================================================================ flush
    /** Flush of all stores. Returns a short report (for shell and log). */
    public String flush(String why) throws IOException {
        checkOpen();
        return internalFlushcache(why);
    }

    /**
     * Corresponds to HRegion.internalFlushcache: (1) under the update lock a snapshot of every MemStore
     * and the flush sequence ID, (2) without lock write the snapshots as HFiles, (3) tell the WAL up to
     * which sequence ID everything is persistent.
     */
    private String internalFlushcache(String why) throws IOException {
        synchronized (flushLock) {
            Map<HStore, DefaultMemStore.MemStoreSnapshot> snapshots = new TreeMap<>(
                    (a, b) -> a.getColumnFamilyName().compareTo(b.getColumnFamilyName()));
            long flushSeqId;
            synchronized (updatesLock) {
                for (HStore s : stores.values()) {
                    snapshots.put(s, s.getMemStore().snapshot());
                }
                flushSeqId = mvcc.getWritePoint();
            }
            StringBuilder report = new StringBuilder();
            int cells = 0;
            for (Map.Entry<HStore, DefaultMemStore.MemStoreSnapshot> e : snapshots.entrySet()) {
                HStoreFile sf = e.getKey().flushSnapshot(e.getValue(), flushSeqId);
                if (sf != null) {
                    cells += e.getValue().getCellsCount();
                    report.append(String.format("  %s/%s: %d cells -> HFile %s (%d bytes)%n",
                            regionInfo.getShortName(), e.getKey().getColumnFamilyName(), e.getValue().getCellsCount(),
                            sf.getName(), sf.length()));
                }
            }
            lastFlushTime = EnvironmentEdgeManager.currentTime();
            lastFlushSeqId = flushSeqId;
            if (wal != null) {
                wal.completeCacheFlush(regionInfo.getEncodedName(), flushSeqId);
            }
            if (cells == 0) {
                return "  " + regionInfo.getShortName() + ": MemStore empty, nothing to flush\n";
            }
            Log.info(LOG, "Finished flush of " + regionInfo.getShortName() + " (" + why + "), sequenceid="
                    + flushSeqId + ", " + cells + " cells");
            if (rsServices != null) {
                for (HStore s : stores.values()) {
                    if (s.needsCompaction()) {
                        rsServices.requestCompaction(this, s, "after flush: " + s.getStorefiles().size()
                                + " HFiles >= hbase.hstore.compactionThreshold="
                                + HBaseConfiguration.COMPACTION_MIN_FILES);
                    }
                }
            }
            return report.toString();
        }
    }

    // =========================================================== compaction
    public List<CompactionRecord> compact(boolean major) throws IOException {
        checkOpen();
        List<CompactionRecord> r = new ArrayList<>();
        for (HStore s : stores.values()) {
            CompactionRecord rec = s.compact(major, major ? "major_compact requested" : "compact requested");
            if (rec != null) {
                r.add(rec);
            }
        }
        return r;
    }

    // =============================================================== close
    /** Clean close: no new writes, flush, close the stores. abort = crash: nothing more is written. */
    public void close(boolean abort) throws IOException {
        closing = true;
        if (!abort && !closed) {
            synchronized (flushLock) {
                internalFlushcache("region close");
            }
        }
        synchronized (updatesLock) {
            for (HStore s : stores.values()) {
                s.close();
            }
            closed = true;
        }
        Log.info(LOG, (abort ? "Aborted " : "Closed ") + regionInfo.getRegionNameAsString());
    }

    private void checkOpen() throws NotServingRegionException {
        if (closing || closed) {
            throw new NotServingRegionException("Region " + regionInfo.getRegionNameAsString() + " is closing");
        }
    }

    private void checkRow(byte[] row) throws IOException {
        if (!regionInfo.containsRow(row)) {
            throw new NotServingRegionException("Requested row out of range for region " + regionInfo
                    + ", row=" + Bytes.toStringBinary(row));
        }
    }

    // ================================================================= info
    public RegionInfo getRegionInfo() {
        return regionInfo;
    }

    public TableDescriptor getTableDescriptor() {
        return htd;
    }

    public Collection<HStore> getStores() {
        return stores.values();
    }

    public HStore getStore(String family) {
        return stores.get(family);
    }

    public long getMemStoreDataSize() {
        long s = 0;
        for (HStore st : stores.values()) {
            s += st.getMemStoreSize();
        }
        return s;
    }

    public long getOpenSeqNum() {
        return openSeqNum;
    }

    public long getReadPoint() {
        return mvcc.getReadPoint();
    }

    public long getLastFlushSeqId() {
        return lastFlushSeqId;
    }

    public long getLastFlushTime() {
        return lastFlushTime;
    }

    public long getReadRequestsCount() {
        return readRequestsCount.get();
    }

    public long getWriteRequestsCount() {
        return writeRequestsCount.get();
    }

    public boolean isClosed() {
        return closed;
    }

    static int compareRows(byte[] a, byte[] b) {
        return CellComparator.compareRows(a, b);
    }
}
