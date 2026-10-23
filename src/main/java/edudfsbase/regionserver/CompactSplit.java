package edudfsbase.regionserver;

import edudfsbase.regionserver.compactions.CompactionRecord;
import edudfs.common.Log;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.CompactSplit: runs compactions in the
 * background (after a flush, if a store has enough HFiles). In the original there are thread pools for
 * "small" and "large" compactions and region splits are scheduled here as well; EduDFSBase has one thread
 * and no splits.
 */
class CompactSplit {

    private final HRegionServer server;
    private final ExecutorService pool;
    private final Set<String> pending = ConcurrentHashMap.newKeySet();

    CompactSplit(HRegionServer server) {
        this.server = server;
        this.pool = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "CompactSplit " + server.getServerName());
            t.setDaemon(true);
            return t;
        });
    }

    void requestCompaction(HRegion region, HStore store, String why) {
        String key = region.getRegionInfo().getEncodedName() + "/" + store.getColumnFamilyName();
        if (!pending.add(key)) {
            return;
        }
        pool.submit(() -> {
            pending.remove(key);
            try {
                if (!region.isClosed()) {
                    CompactionRecord r = store.compact(false, why);
                    if (r != null) {
                        server.addCompactionRecord(r);
                    }
                }
            } catch (Exception e) {
                Log.warn("CompactSplit", "Compaction of " + key + " failed: " + e.getMessage());
            }
        });
    }

    void shutdown() {
        pool.shutdownNow();
    }
}
