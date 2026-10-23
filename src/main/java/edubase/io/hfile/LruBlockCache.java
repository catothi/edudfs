package edubase.io.hfile;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.LruBlockCache: ONE cache per RegionServer for
 * the blocks of all its HFiles. Key = (HFile name, offset of the block).
 *
 * A read first looks into the cache (hit) - only on a miss does the block come from HDFS. Once
 * the cache is full, the least recently used block is evicted (LRU).
 *
 * Simplification: the original splits the cache into three priorities (single access,
 * multiple access, in-memory families) so that a single large scan does not push all
 * frequently used blocks out. Here: plain LRU over a LinkedHashMap in access order.
 */
public class LruBlockCache {

    /** Corresponds to BlockCacheKey. */
    public record BlockCacheKey(String hfileName, long offset) {
    }

    private final long maxSize;
    private long size;
    private final LinkedHashMap<BlockCacheKey, HFileBlock> map = new LinkedHashMap<>(16, 0.75f, true);

    // CacheStats
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();
    private final AtomicLong evictedCount = new AtomicLong();

    public LruBlockCache(long maxSize) {
        this.maxSize = maxSize;
    }

    public synchronized HFileBlock getBlock(BlockCacheKey key) {
        HFileBlock b = map.get(key);
        if (b == null) {
            missCount.incrementAndGet();
        } else {
            hitCount.incrementAndGet();
        }
        return b;
    }

    public synchronized void cacheBlock(BlockCacheKey key, HFileBlock block) {
        if (map.put(key, block) == null) {
            size += block.getOnDiskSizeWithHeader();
        }
        Iterator<Map.Entry<BlockCacheKey, HFileBlock>> it = map.entrySet().iterator();
        while (size > maxSize && it.hasNext()) {
            Map.Entry<BlockCacheKey, HFileBlock> eldest = it.next();
            size -= eldest.getValue().getOnDiskSizeWithHeader();
            it.remove();
            evictedCount.incrementAndGet();
        }
    }

    /** Called when an HFile goes away (compaction): its blocks are no longer needed. */
    public synchronized int evictBlocksByHfileName(String hfileName) {
        int n = 0;
        Iterator<Map.Entry<BlockCacheKey, HFileBlock>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<BlockCacheKey, HFileBlock> e = it.next();
            if (e.getKey().hfileName().equals(hfileName)) {
                size -= e.getValue().getOnDiskSizeWithHeader();
                it.remove();
                n++;
            }
        }
        return n;
    }

    public synchronized long getCurrentSize() {
        return size;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public synchronized int getBlockCount() {
        return map.size();
    }

    public long getHitCount() {
        return hitCount.get();
    }

    public long getMissCount() {
        return missCount.get();
    }

    public long getEvictedCount() {
        return evictedCount.get();
    }

    @Override
    public String toString() {
        long requests = getHitCount() + getMissCount();
        return String.format("blocks=%d, size=%d of %d bytes, hits=%d, misses=%d, hitRatio=%s, evicted=%d",
                getBlockCount(), getCurrentSize(), maxSize, getHitCount(), getMissCount(),
                requests == 0 ? "-" : String.format(Locale.ROOT, "%.0f%%", 100.0 * getHitCount() / requests),
                getEvictedCount());
    }
}
