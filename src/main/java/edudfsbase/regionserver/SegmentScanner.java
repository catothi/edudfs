package edudfsbase.regionserver;

import edudfsbase.Cell;

import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.SegmentScanner: scans a MemStore segment
 * (active or snapshot). Cells whose sequence ID is above the read point are skipped - they
 * belong to a write that is not complete yet (MVCC).
 */
public class SegmentScanner implements KeyValueScanner {

    private final NavigableSet<Cell> cells;
    private final long readPoint;
    private final String sourceName;
    private Iterator<Cell> it;
    private Cell current;

    public SegmentScanner(NavigableSet<Cell> cells, long readPoint, String sourceName) {
        this.cells = cells;
        this.readPoint = readPoint;
        this.sourceName = sourceName;
    }

    private void advance() {
        current = null;
        while (it.hasNext()) {
            Cell c = it.next();
            if (c.getSequenceId() <= readPoint) {
                current = c;
                return;
            }
        }
    }

    @Override
    public Cell peek() {
        return current;
    }

    @Override
    public Cell next() {
        Cell c = current;
        if (c != null) {
            advance();
        }
        return c;
    }

    @Override
    public boolean seek(Cell key) {
        it = cells.tailSet(key, true).iterator();
        advance();
        return current != null;
    }

    @Override
    public boolean reseek(Cell key) {
        return seek(key);  // a skip list seeks in O(log n), forward or backward alike
    }

    @Override
    public void close() {
        current = null;
    }

    @Override
    public String getSourceName() {
        return sourceName;
    }
}
