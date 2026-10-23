package edubase.io.hfile;

import edubase.Cell;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFileScanner: a cursor over the cells of an HFile.
 *
 * Difference from the original: seekTo(key) here positions AT OR AFTER the key (the original
 * positions at or before it and leaves the rest to StoreFileScanner.seekAtOrAfter).
 */
public interface HFileScanner {

    /** To the first cell of the file. false if the file is empty. */
    boolean seekTo() throws IOException;

    /** To the first cell &gt;= key: block index -&gt; block -&gt; binary search in the block. false = none left. */
    boolean seekTo(Cell key) throws IOException;

    /** Like seekTo(key), but only forward; stays in the current block if the key lies in it. */
    boolean reseekTo(Cell key) throws IOException;

    /** To the next cell; false at the end of the file. */
    boolean next() throws IOException;

    Cell getCell();

    boolean isSeeked();
}
