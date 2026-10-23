package edubase.regionserver;

import edubase.Cell;
import edubase.CellComparator;
import edubase.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.DefaultMemStore: the in-memory part of a store
 * (one per column family and region).
 *
 * New cells go into the ACTIVE segment, a ConcurrentSkipListSet sorted by the CellComparator
 * - so the MemStore is always sorted, and a flush only has to write it out from front to back.
 *
 * Flush in two steps (as in the original):
 * <ol>
 *   <li>{@link #snapshot()}: the active segment becomes the SNAPSHOT, a new empty active segment
 *       takes the writes. This is quick and happens under the region's update lock.</li>
 *   <li>The snapshot is written to an HFile (slow, without lock - readers still see it).
 *       Afterwards {@link #clearSnapshot} drops it.</li>
 * </ol>
 * Simplification: the original manages segments as objects of their own (MutableSegment,
 * ImmutableSegment) and can additionally compact in memory (CompactingMemStore).
 */
public class DefaultMemStore {

    /** Rough overhead per cell in the skip list (original: ClassSize, ~100 bytes). Only for the display. */
    static final int CELL_HEAP_OVERHEAD = 100;

    private volatile ConcurrentSkipListSet<Cell> active = newSegment();
    private volatile ConcurrentSkipListSet<Cell> snapshot = newSegment();
    private final AtomicLong activeDataSize = new AtomicLong();
    private volatile long snapshotDataSize;
    private long snapshotId = -1;

    private static ConcurrentSkipListSet<Cell> newSegment() {
        return new ConcurrentSkipListSet<>(CellComparator.getInstance());
    }

    /** Snapshot of the MemStore that is being flushed (original: MemStoreSnapshot). */
    public static final class MemStoreSnapshot {
        private final long id;
        private final NavigableSet<Cell> cells;
        private final long dataSize;

        MemStoreSnapshot(long id, NavigableSet<Cell> cells, long dataSize) {
            this.id = id;
            this.cells = cells;
            this.dataSize = dataSize;
        }

        public long getId() {
            return id;
        }

        public NavigableSet<Cell> getCells() {
            return cells;
        }

        public int getCellsCount() {
            return cells.size();
        }

        public long getDataSize() {
            return dataSize;
        }
    }

    public void add(Cell cell) {
        active.add(cell);
        activeDataSize.addAndGet(((KeyValue) cell).getSerializedSize());
    }

    /** Active segment -&gt; snapshot. Must be called under the region's update lock. */
    public synchronized MemStoreSnapshot snapshot() {
        if (!snapshot.isEmpty()) {
            // The previous flush failed - flush the old snapshot again first
            return new MemStoreSnapshot(snapshotId, snapshot, snapshotDataSize);
        }
        snapshotId = System.nanoTime();
        snapshot = active;
        snapshotDataSize = activeDataSize.getAndSet(0);
        active = newSegment();
        return new MemStoreSnapshot(snapshotId, snapshot, snapshotDataSize);
    }

    /** The snapshot is in an HFile now. Readers that still hold it keep their reference. */
    public synchronized void clearSnapshot(long id) {
        if (id == snapshotId) {
            snapshot = newSegment();
            snapshotDataSize = 0;
        }
    }

    public List<KeyValueScanner> getScanners(long readPoint) {
        List<KeyValueScanner> r = new ArrayList<>(2);
        r.add(new SegmentScanner(active, readPoint, "MemStore"));
        ConcurrentSkipListSet<Cell> snap = snapshot;
        if (!snap.isEmpty()) {
            r.add(new SegmentScanner(snap, readPoint, "MemStore snapshot"));
        }
        return r;
    }

    /** Data size of active segment + snapshot (keys + values in the binary layout). */
    public long getDataSize() {
        return activeDataSize.get() + snapshotDataSize;
    }

    public long getActiveDataSize() {
        return activeDataSize.get();
    }

    /** Approximate heap usage (for the display; the original computes this exactly). */
    public long getHeapSize() {
        return getDataSize() + (long) (active.size() + snapshot.size()) * CELL_HEAP_OVERHEAD;
    }

    public int getActiveCellsCount() {
        return active.size();
    }

    public int getSnapshotCellsCount() {
        return snapshot.size();
    }

    /** For metasave: the cells of the active segment (sorted). */
    public NavigableSet<Cell> getActiveCells() {
        return active;
    }
}
