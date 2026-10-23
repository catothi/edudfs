package edubase.regionserver;

import edudfs.common.Log;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.MemStoreFlusher: a thread that executes flush
 * requests. A region asks for a flush after a write once its MemStore has exceeded
 * hbase.hregion.memstore.flush.size; the write itself does not wait for it.
 *
 * Simplification: the original additionally watches the total MemStore size of the server
 * (hbase.regionserver.global.memstore.size) and blocks writes when it is exceeded.
 */
class MemStoreFlusher extends Thread {

    private record FlushRequest(HRegion region, String why) {
    }

    private final LinkedBlockingQueue<FlushRequest> flushQueue = new LinkedBlockingQueue<>();
    private final Set<String> queued = ConcurrentHashMap.newKeySet();
    private volatile boolean stopped;

    MemStoreFlusher(String serverName) {
        super("MemStoreFlusher " + serverName);
        setDaemon(true);
    }

    void requestFlush(HRegion region, String why) {
        if (queued.add(region.getRegionInfo().getEncodedName())) {
            flushQueue.add(new FlushRequest(region, why));
        }
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                FlushRequest r = flushQueue.poll(1, TimeUnit.SECONDS);
                if (r == null) {
                    continue;
                }
                queued.remove(r.region().getRegionInfo().getEncodedName());
                if (!r.region().isClosed()) {
                    r.region().flush(r.why());
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (!stopped) {
                    Log.warn("MemStoreFlusher", "Flush failed: " + e.getMessage());
                }
            }
        }
    }

    void shutdown() {
        stopped = true;
        interrupt();
    }
}
