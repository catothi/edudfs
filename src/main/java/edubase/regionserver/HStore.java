package edubase.regionserver;

import edubase.Cell;
import edubase.HConstants;
import edubase.KeyValue;
import edubase.client.ColumnFamilyDescriptor;
import edubase.client.Scan;
import edubase.fs.HFileSystem;
import edubase.io.hfile.LruBlockCache;
import edubase.regionserver.compactions.CompactionRecord;
import edubase.regionserver.compactions.CompactionRequest;
import edubase.regionserver.compactions.Compactor;
import edubase.regionserver.compactions.RatioBasedCompactionPolicy;
import edubase.util.Bytes;
import edubase.util.EnvironmentEdgeManager;
import edubase.util.ReadTrace;
import edudfs.client.DFSOutputStream;
import edudfs.common.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.HStore: ONE column family of ONE region.
 * A store = MemStore + n HFiles (in HDFS under &lt;region&gt;/&lt;family&gt;/) - the two levels of an LSM tree.
 *
 * <ul>
 *   <li>Write: cell into the MemStore (the WAL was already written by the region).</li>
 *   <li>Flush: MemStore snapshot -&gt; new HFile. The number of HFiles grows.</li>
 *   <li>Read: MemStore + all HFiles merged (StoreScanner).</li>
 *   <li>Compaction: several HFiles -&gt; one. The number shrinks, reads get cheaper.</li>
 * </ul>
 */
public class HStore {

    private static final String LOG = "HStore";

    private final HRegion region;
    private final ColumnFamilyDescriptor family;
    private final HRegionFileSystem fs;
    private final LruBlockCache blockCache;
    private final DefaultMemStore memstore = new DefaultMemStore();
    /** The HFiles, oldest (smallest max sequence ID) first. Replaced as a whole (copy-on-write). */
    private volatile List<HStoreFile> storefiles = List.of();
    private final Object compactionLock = new Object();
    private long flushCount;
    private long compactionCount;

    HStore(HRegion region, ColumnFamilyDescriptor family, HRegionFileSystem fs, LruBlockCache blockCache)
            throws IOException {
        this.region = region;
        this.family = family;
        this.fs = fs;
        this.blockCache = blockCache;
        List<HStoreFile> files = new ArrayList<>();
        for (String path : fs.getStoreFiles(getColumnFamilyName())) {
            files.add(new HStoreFile(fs.getFileSystem(), path, blockCache));
        }
        files.sort(Comparator.comparingLong(HStoreFile::getMaxSequenceId));
        this.storefiles = Collections.unmodifiableList(files);
    }

    public String getColumnFamilyName() {
        return family.getNameAsString();
    }

    public ColumnFamilyDescriptor getColumnFamilyDescriptor() {
        return family;
    }

    public HRegionFileSystem getRegionFileSystem() {
        return fs;
    }

    public HRegion getRegion() {
        return region;
    }

    void add(Cell cell) {
        memstore.add(cell);
    }

    DefaultMemStore getMemStore() {
        return memstore;
    }

    /** Highest sequence ID contained in an HFile (0 if there is none). Edits up to it are persistent. */
    public long getMaxSequenceId() {
        long max = 0;
        for (HStoreFile f : storefiles) {
            max = Math.max(max, f.getMaxSequenceId());
        }
        return max;
    }

    public List<HStoreFile> getStorefiles() {
        return storefiles;
    }

    // ================================================================ flush
    /**
     * Writes a MemStore snapshot into a new HFile: .tmp/&lt;family&gt;/&lt;uuid&gt; -&gt; commit (rename)
     * -&gt; open -&gt; add to the list -&gt; drop the snapshot. Corresponds to StoreFlusher.flushSnapshot +
     * HStore.commitFile (the original's StoreFlushContext: prepare / flushCache / commit).
     */
    HStoreFile flushSnapshot(DefaultMemStore.MemStoreSnapshot snapshot, long flushSeqId) throws IOException {
        if (snapshot.getCellsCount() == 0) {
            return null;
        }
        HFileSystem hfs = fs.getFileSystem();
        String tmp = fs.createTempPath(getColumnFamilyName());
        try (DFSOutputStream out = hfs.create(tmp, false);
             StoreFileWriter writer = new StoreFileWriter(out, tmp, family, snapshot.getCellsCount())) {
            for (Cell c : snapshot.getCells()) {
                writer.append(c);
            }
            writer.appendMetadata(flushSeqId, false, "flush", null);
        }
        String path = fs.commitStoreFile(getColumnFamilyName(), tmp);
        HStoreFile sf = new HStoreFile(hfs, path, blockCache);
        synchronized (this) {
            List<HStoreFile> n = new ArrayList<>(storefiles);
            n.add(sf);
            storefiles = Collections.unmodifiableList(n);
        }
        memstore.clearSnapshot(snapshot.getId());
        flushCount++;
        Log.info(LOG, "Flushed " + region.getRegionInfo().getShortName() + "/" + getColumnFamilyName() + ": "
                + snapshot.getCellsCount() + " cells, " + snapshot.getDataSize() + " bytes in MemStore -> HFile "
                + sf.getName() + " (" + sf.length() + " bytes, " + sf.getReader().getEntries()
                + " cells), sequenceid=" + flushSeqId);
        return sf;
    }

    // ================================================================= read
    /** MemStore scanners + scanners of the HFiles that can be relevant (time/key range, Bloom). */
    List<KeyValueScanner> getScanners(Scan scan, NavigableSet<String> columns, long readPt, long oldestUnexpiredTS,
                                      ReadTrace trace) {
        List<KeyValueScanner> r = new ArrayList<>(memstore.getScanners(readPt));
        if (trace != null) {
            trace.add("store " + getColumnFamilyName() + ": MemStore (" + memstore.getActiveCellsCount() + " cells"
                    + (memstore.getSnapshotCellsCount() > 0 ? ", snapshot " + memstore.getSnapshotCellsCount() : "")
                    + ") + " + storefiles.size() + " HFile(s)");
        }
        // newest file first, so the trace reads in the order a Get "thinks"
        List<HStoreFile> files = new ArrayList<>(storefiles);
        Collections.reverse(files);
        for (HStoreFile f : files) {
            StoreFileScanner s = new StoreFileScanner(f, family.isBlockCacheEnabled(), trace);
            if (s.shouldUseScanner(scan, columns, oldestUnexpiredTS)) {
                r.add(s);
            } else {
                s.close();
            }
        }
        return r;
    }

    StoreScanner getScanner(Scan scan, NavigableSet<String> columns, long readPt, ReadTrace trace) throws IOException {
        long now = EnvironmentEdgeManager.currentTime();
        long ttl = family.getTimeToLive();
        long oldestUnexpiredTS = ttl == HConstants.FOREVER ? Long.MIN_VALUE : now - ttl * 1000;
        List<KeyValueScanner> scanners = getScanners(scan, columns, readPt, oldestUnexpiredTS, trace);
        Cell seekKey = scan.isGetScan() && columns != null && columns.size() == 1
                ? KeyValue.createFirstOnRowCol(scan.getStartRow(), family.getName(),
                Bytes.toBytesBinary(columns.first()))
                : KeyValue.createFirstOnRow(scan.getStartRow());
        for (KeyValueScanner s : scanners) {
            s.seek(seekKey);
        }
        int versions = scan.isRaw() ? scan.getMaxVersions() : Math.min(scan.getMaxVersions(), family.getMaxVersions());
        ScanQueryMatcher matcher = new ScanQueryMatcher(ScanQueryMatcher.ScanType.USER_SCAN, scan.isRaw(), versions,
                scan.getTimeRange(), ttl, now, columns);
        return new StoreScanner(scanners, matcher);
    }

    // =========================================================== compaction
    public boolean needsCompaction() {
        return RatioBasedCompactionPolicy.needsCompaction(storefiles);
    }

    /**
     * Selects files, compacts them, swaps them. null if the policy selects nothing.
     * Corresponds to HStore.requestCompaction + compact + replaceStoreFiles + completeCompaction.
     */
    public CompactionRecord compact(boolean major, String why) throws IOException {
        synchronized (compactionLock) {  // at most one compaction per store at a time
            CompactionRequest request = RatioBasedCompactionPolicy.selectCompaction(storefiles, major, why);
            if (request == null) {
                return null;
            }
            long start = EnvironmentEdgeManager.currentTime();
            Log.info(LOG, "Starting " + (request.isMajor() ? "major" : "minor") + " compaction of "
                    + request.getFiles().size() + " file(s) in " + getColumnFamilyName() + " of "
                    + region.getRegionInfo().getRegionNameAsString() + " (" + why + ")");
            CompactionRecord record = new CompactionRecord(region.getRegionInfo().getTable() + ","
                    + region.getRegionInfo().getShortName(), getColumnFamilyName(), why);
            String tmp = Compactor.compact(this, request, record);
            String path = fs.commitStoreFile(getColumnFamilyName(), tmp);
            HStoreFile result = new HStoreFile(fs.getFileSystem(), path, blockCache);
            List<String> oldPaths = new ArrayList<>();
            synchronized (this) {
                List<HStoreFile> n = new ArrayList<>(storefiles);
                n.removeAll(request.getFiles());
                n.add(result);
                n.sort(Comparator.comparingLong(HStoreFile::getMaxSequenceId));
                storefiles = Collections.unmodifiableList(n);
            }
            for (HStoreFile f : request.getFiles()) {
                oldPaths.add(f.getPath());
                if (blockCache != null) {
                    blockCache.evictBlocksByHfileName(f.getName());
                }
                f.markCompactedAway();
            }
            fs.removeStoreFiles(getColumnFamilyName(), oldPaths);
            compactionCount++;
            Compactor.finish(record, result, storefiles.size(), start);
            Log.info(LOG, "Completed " + (request.isMajor() ? "major" : "minor") + " compaction of "
                    + request.getFiles().size() + " file(s) in " + getColumnFamilyName() + " of "
                    + region.getRegionInfo().getShortName() + " into " + result.getName() + " ("
                    + result.length() + " bytes)");
            return record;
        }
    }

    // ================================================================== info
    public long getMemStoreSize() {
        return memstore.getDataSize();
    }

    public long getStorefilesSize() {
        long s = 0;
        for (HStoreFile f : storefiles) {
            s += f.length();
        }
        return s;
    }

    public long getFlushCount() {
        return flushCount;
    }

    public long getCompactionCount() {
        return compactionCount;
    }

    void close() {
        for (HStoreFile f : storefiles) {
            f.closeStoreFile();
        }
    }
}
