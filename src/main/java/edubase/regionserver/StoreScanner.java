package edubase.regionserver;

import edubase.Cell;
import edubase.KeyValue;
import edubase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.StoreScanner: reads ONE store (column family)
 * row by row: KeyValueHeap (MemStore + HFiles merged) -&gt; ScanQueryMatcher (versions, deletes,
 * TTL, columns) -&gt; cells of the row.
 *
 * Used for user scans/gets as well as for compactions; only the ScanQueryMatcher differs.
 */
public class StoreScanner {

    private final KeyValueHeap heap;
    private final ScanQueryMatcher matcher;

    /** @param scanners already positioned (seek) on the start key */
    public StoreScanner(List<? extends KeyValueScanner> scanners, ScanQueryMatcher matcher) {
        this.heap = new KeyValueHeap(scanners);
        this.matcher = matcher;
    }

    /** The next cell (to compare rows across several stores); null if exhausted. */
    public Cell peek() {
        return heap.peek();
    }

    /**
     * Returns the cells of the current row (sources: parallel list "MemStore"/"HFile ..." or null).
     * @return true if more rows follow
     */
    public boolean next(List<Cell> out, List<String> sources) throws IOException {
        Cell c = heap.peek();
        if (c == null) {
            return false;
        }
        byte[] row = c.getRow();
        while ((c = heap.peek()) != null && Bytes.equals(c.getRow(), row)) {
            String source = heap.getCurrentSourceName();
            switch (matcher.match(c)) {
                case INCLUDE -> {
                    out.add(c);
                    if (sources != null) {
                        sources.add(source);
                    }
                    heap.next();
                }
                case SKIP -> heap.next();
                case SEEK_NEXT_COL ->
                        heap.reseek(KeyValue.createLastOnRowCol(c.getRow(), c.getFamily(), c.getQualifier()));
                default -> throw new IllegalStateException();
            }
        }
        return heap.peek() != null;
    }

    public ScanQueryMatcher getMatcher() {
        return matcher;
    }

    public void close() {
        heap.close();
    }
}
