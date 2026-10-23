package edudfsbase;

/**
 * Corresponds to org.apache.hadoop.hbase.Cell: the smallest unit of data in HBase.
 *
 * A cell is addressed by the KEY (row, family, qualifier, timestamp, type) and carries a value.
 * There is no "update in place": a new value is a new cell with a newer timestamp, and a
 * delete is a new cell of a delete type (a "tombstone").
 *
 * Simplification: the original exposes every field as (array, offset, length) so that cells can
 * point into a shared buffer without copying. Here every getter returns its own byte[].
 */
public interface Cell {

    byte[] getRow();

    byte[] getFamily();

    byte[] getQualifier();

    long getTimestamp();

    KeyValue.Type getType();

    byte[] getValue();

    /**
     * The sequence ID (= MVCC write number) of the edit that wrote the cell. Assigned by the region
     * on write; a reader only sees cells whose sequence ID is &lt;= its read point.
     */
    long getSequenceId();
}
