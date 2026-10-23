package edudfsbase.wal;

import edudfsbase.TableName;

/**
 * Corresponds to org.apache.hadoop.hbase.wal.WALKeyImpl: the header of a WAL entry - to which
 * region it belongs and which sequence ID the edit has. Since ALL regions of a RegionServer
 * write into the same WAL, the encoded region name is what lets the WAL be split later.
 */
public class WALKeyImpl {

    private final String encodedRegionName;
    private final TableName tableName;
    private final long sequenceId;
    private final long writeTime;

    public WALKeyImpl(String encodedRegionName, TableName tableName, long sequenceId, long writeTime) {
        this.encodedRegionName = encodedRegionName;
        this.tableName = tableName;
        this.sequenceId = sequenceId;
        this.writeTime = writeTime;
    }

    public String getEncodedRegionName() {
        return encodedRegionName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public long getWriteTime() {
        return writeTime;
    }

    @Override
    public String toString() {
        return tableName + "/" + encodedRegionName + "/" + sequenceId;
    }
}
