package edubase.wal;

import edubase.HBaseConfiguration;
import edubase.RegionInfo;
import edubase.ServerName;
import edubase.fs.CommonFSUtils;
import edubase.fs.HFileSystem;
import edubase.util.EnvironmentEdgeManager;
import edudfs.common.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.wal.FSHLog: the WAL of a RegionServer as a
 * sequence of files in HDFS under /hbase/WALs/&lt;servername&gt;/.
 *
 * <ul>
 *   <li><b>One WAL for all regions</b> of the server: writes stay sequential (one file, one HDFS
 *       pipeline), which is fast. Price: after a crash the WAL must be SPLIT by region.</li>
 *   <li><b>Sync = hflush</b>: an edit is acknowledged to the client only once it is in the memory
 *       of all DataNodes of the pipeline.</li>
 *   <li><b>Roll</b>: from a certain size (hlog.blocksize x logroll.multiplier) or age a new file is
 *       started (LogRoller).</li>
 *   <li><b>Archiving</b>: for every WAL file the highest sequence ID per region is noted
 *       (SequenceIdAccounting). Once every region has flushed beyond it, the file is no longer needed
 *       and moves to /hbase/oldWALs.</li>
 * </ul>
 * Simplification: the original decouples append and sync via a ring buffer (LMAX Disruptor) and syncs
 * many edits together ("group commit"); here every mutation is synced on its own.
 */
public class FSHLog implements WAL {

    private static final String LOG = "FSHLog";

    private final HFileSystem fs;
    private final String walDir;
    private final String prefix;
    private final long logrollSize;

    private TextLogWriter writer;
    private String currentPath;
    private long currentCreateTime;
    private long currentEntries;
    /** Highest sequence ID per region in the CURRENT file. */
    private Map<String, Long> currentRegionSeqIds = new HashMap<>();
    /** Rolled files (oldest first) -&gt; highest sequence ID per region in them. */
    private final LinkedHashMap<String, Map<String, Long>> rolledWALs = new LinkedHashMap<>();
    /** Per region: up to which sequence ID everything is in HFiles. */
    private final Map<String, Long> flushedSeqIds = new HashMap<>();
    private final List<String> archived = new ArrayList<>();
    private boolean closed;

    public FSHLog(HFileSystem fs, ServerName serverName) throws IOException {
        this.fs = fs;
        this.walDir = CommonFSUtils.getWALDirectoryName(serverName);
        this.prefix = CommonFSUtils.getWALPrefix(serverName);
        this.logrollSize = (long) (HBaseConfiguration.WAL_BLOCK_SIZE * HBaseConfiguration.LOGROLL_MULTIPLIER);
        fs.mkdirs(walDir);
        fs.mkdirs(CommonFSUtils.getOldWALDir());
        rollWriter();
    }

    @Override
    public synchronized long appendData(RegionInfo region, WALKeyImpl key, WALEdit edit) throws IOException {
        checkOpen();
        writer.append(new Entry(key, edit));
        currentRegionSeqIds.merge(key.getEncodedRegionName(), key.getSequenceId(), Math::max);
        currentEntries++;
        return key.getSequenceId();
    }

    @Override
    public synchronized void sync() throws IOException {
        checkOpen();
        writer.sync();
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("WAL closed: " + walDir);
        }
    }

    /** Should the LogRoller start a new file? */
    public synchronized boolean isRollRequested() {
        return !closed && currentEntries > 0 && (writer.getLength() >= logrollSize
                || EnvironmentEdgeManager.currentTime() - currentCreateTime >= HBaseConfiguration.LOGROLL_PERIOD);
    }

    @Override
    public synchronized void rollWriter() throws IOException {
        checkOpen();
        String oldPath = currentPath;
        if (writer != null) {
            writer.close();
            rolledWALs.put(currentPath, currentRegionSeqIds);
        }
        long now = EnvironmentEdgeManager.currentTime();
        if (now <= currentCreateTime) {
            now = currentCreateTime + 1;  // file names must be unique
        }
        currentCreateTime = now;
        currentPath = walDir + "/" + prefix + "." + now;
        writer = new TextLogWriter(fs.create(currentPath, false, HBaseConfiguration.WAL_BLOCK_SIZE));
        writer.sync();
        currentRegionSeqIds = new HashMap<>();
        currentEntries = 0;
        if (oldPath != null) {
            Log.info(LOG, "Rolled WAL " + CommonFSUtils.getName(oldPath) + " -> new WAL " + CommonFSUtils.getName(currentPath));
        } else {
            Log.info(LOG, "New WAL " + currentPath);
        }
        cleanOldLogs();
    }

    @Override
    public synchronized void completeCacheFlush(String encodedRegionName, long flushedSeqId) {
        flushedSeqIds.merge(encodedRegionName, flushedSeqId, Math::max);
        cleanOldLogs();
    }

    /** Corresponds to AbstractFSWAL.cleanOldLogs: move WAL files whose edits are all flushed to oldWALs. */
    private void cleanOldLogs() {
        Iterator<Map.Entry<String, Map<String, Long>>> it = rolledWALs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Long>> e = it.next();
            String blockingRegion = null;
            for (Map.Entry<String, Long> r : e.getValue().entrySet()) {
                if (flushedSeqIds.getOrDefault(r.getKey(), -1L) < r.getValue()) {
                    blockingRegion = r.getKey();
                    break;
                }
            }
            if (blockingRegion != null) {
                continue;  // still needed: this region has unflushed edits in it
            }
            String dst = CommonFSUtils.getOldWALDir() + "/" + CommonFSUtils.getName(e.getKey());
            try {
                fs.rename(e.getKey(), dst);
                archived.add(CommonFSUtils.getName(e.getKey()));
                Log.info(LOG, "Archiving WAL " + CommonFSUtils.getName(e.getKey()) + " to oldWALs "
                        + "(all its edits are flushed into HFiles)");
                it.remove();
            } catch (IOException ex) {
                Log.warn(LOG, "Failed to archive " + e.getKey() + ": " + ex.getMessage());
            }
        }
    }

    /** Clean shutdown: close the current file; with every region flushed, all files go to oldWALs. */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        writer.close();
        rolledWALs.put(currentPath, currentRegionSeqIds);
        closed = true;
        cleanOldLogs();
        if (rolledWALs.isEmpty()) {
            fs.delete(walDir, true);
        }
    }

    // ------------------------------------------------------------ info
    public synchronized String getCurrentFileName() {
        return currentPath;
    }

    public synchronized long getCurrentFileLength() {
        return writer.getLength();
    }

    public synchronized long getCurrentEntries() {
        return currentEntries;
    }

    public long getLogrollSize() {
        return logrollSize;
    }

    /** For metasave: rolled but not yet archivable files and why. */
    public synchronized List<String> describeRolledWALs() {
        List<String> r = new ArrayList<>();
        for (Map.Entry<String, Map<String, Long>> e : rolledWALs.entrySet()) {
            StringBuilder why = new StringBuilder();
            for (Map.Entry<String, Long> reg : e.getValue().entrySet()) {
                long flushed = flushedSeqIds.getOrDefault(reg.getKey(), -1L);
                if (flushed < reg.getValue()) {
                    why.append(why.length() > 0 ? ", " : "").append(reg.getKey(), 0, 8).append(" has edits up to seqid ")
                            .append(reg.getValue()).append(", flushed only up to ").append(flushed);
                }
            }
            r.add(CommonFSUtils.getName(e.getKey()) + "  waiting for flush: " + why);
        }
        return r;
    }

    public synchronized List<String> getArchived() {
        return new ArrayList<>(archived);
    }
}
