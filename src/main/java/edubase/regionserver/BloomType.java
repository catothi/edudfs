package edubase.regionserver;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.BloomType: which key goes into the Bloom
 * filter of an HFile (column family attribute BLOOMFILTER, original default ROW).
 */
public enum BloomType {
    /** No Bloom filter. Every Get must look into every HFile (index + block). */
    NONE,
    /** Key = row. Answers "can this file contain the row at all?". */
    ROW,
    /**
     * Key = row + qualifier. Answers "can this file contain this column of the row?" - only helps
     * for a Get on exactly one column, but then also skips files that contain the row with
     * other columns.
     */
    ROWCOL
}
