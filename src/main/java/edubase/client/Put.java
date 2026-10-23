package edubase.client;

import edubase.HConstants;
import edubase.KeyValue;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Put: writes cells into one row.
 * Without an explicit timestamp the RegionServer sets its current time on arrival.
 */
public class Put extends Mutation {

    private static final long serialVersionUID = 1L;

    public Put(byte[] row) {
        super(row, HConstants.LATEST_TIMESTAMP);
    }

    public Put(byte[] row, long ts) {
        super(row, ts);
    }

    public Put addColumn(byte[] family, byte[] qualifier, byte[] value) {
        return addColumn(family, qualifier, ts, value);
    }

    public Put addColumn(byte[] family, byte[] qualifier, long ts, byte[] value) {
        cells.add(new KeyValue(row, family, qualifier, ts, KeyValue.Type.Put, value));
        return this;
    }
}
