package edubase.client;

import edubase.HConstants;
import edubase.KeyValue;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Delete.
 *
 * A delete removes nothing right away - it writes a delete marker ("tombstone"). Readers hide
 * everything the marker covers; only a compaction over ALL HFiles of the store (major compaction, or a
 * minor one that happened to select all files) removes the marker and the covered cells physically.
 * Marker types:
 * <table>
 *   <caption>delete markers</caption>
 *   <tr><td>addColumn(f, q)</td><td>Delete</td><td>exactly one version (the newest, or the one with ts)</td></tr>
 *   <tr><td>addColumns(f, q)</td><td>DeleteColumn</td><td>all versions of the column with timestamp &lt;= ts</td></tr>
 *   <tr><td>addFamily(f)</td><td>DeleteFamily</td>
 *       <td>all columns of the family in this row with timestamp &lt;= ts</td></tr>
 *   <tr><td>new Delete(row) alone</td><td>DeleteFamily</td><td>for every family of the table (whole row)</td></tr>
 * </table>
 */
public class Delete extends Mutation {

    private static final long serialVersionUID = 1L;

    public Delete(byte[] row) {
        super(row, HConstants.LATEST_TIMESTAMP);
    }

    public Delete(byte[] row, long ts) {
        super(row, ts);
    }

    public Delete addFamily(byte[] family) {
        cells.add(new KeyValue(row, family, null, ts, KeyValue.Type.DeleteFamily, null));
        return this;
    }

    public Delete addColumns(byte[] family, byte[] qualifier) {
        return addColumns(family, qualifier, ts);
    }

    public Delete addColumns(byte[] family, byte[] qualifier, long ts) {
        cells.add(new KeyValue(row, family, qualifier, ts, KeyValue.Type.DeleteColumn, null));
        return this;
    }

    /** Newest version of the column (the RegionServer looks up its timestamp). */
    public Delete addColumn(byte[] family, byte[] qualifier) {
        return addColumn(family, qualifier, HConstants.LATEST_TIMESTAMP);
    }

    public Delete addColumn(byte[] family, byte[] qualifier, long ts) {
        cells.add(new KeyValue(row, family, qualifier, ts, KeyValue.Type.Delete, null));
        return this;
    }
}
