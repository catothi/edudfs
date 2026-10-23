package edubase.regionserver;

import edubase.Cell;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.KeyValueScanner: a sorted stream of cells.
 * Implementations: the MemStore ({@link SegmentScanner}), an HFile ({@link StoreFileScanner}) and
 * the merge of several scanners ({@link KeyValueHeap}).
 */
public interface KeyValueScanner {

    /** The current cell without moving on; null at the end. */
    Cell peek();

    /** The current cell, then move on. */
    Cell next() throws IOException;

    /** Position at the first cell &gt;= key (any direction). false if there is none. */
    boolean seek(Cell key) throws IOException;

    /** Like seek, but only forward (cheaper: an HFile scanner can stay in its block). */
    boolean reseek(Cell key) throws IOException;

    void close();

    /** EduBase: where the cells come from ("MemStore", "HFile 3f2a9c01"). */
    String getSourceName();
}
