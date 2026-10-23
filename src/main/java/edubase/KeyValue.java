package edubase;

import edubase.util.Bytes;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hbase.KeyValue, the standard implementation of {@link Cell}.
 *
 * In the original a KeyValue is ONE byte[] with this layout (the same in memory, in the HFile
 * and in the WAL):
 * <pre>
 *   int keylength | int valuelength | short rowlength | row | byte familylength | family
 *   | qualifier | long timestamp | byte type | value
 * </pre>
 * The qualifier has no length field - it is "the rest" of the key. EduBase keeps the fields
 * separately and writes them as TEXT (see {@link edubase.codec.KeyValueCodec}); the size
 * calculation {@link #getSerializedSize()} still follows the binary layout.
 */
public class KeyValue implements Cell, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Cell types with the original codes. The order matters: for the same row/column/timestamp the
     * type with the HIGHER code sorts first, so a reader sees a delete marker before the cells it
     * deletes.
     */
    public enum Type {
        Minimum(0), Put(4), Delete(8), DeleteColumn(12), DeleteFamily(14), Maximum(255);

        private final int code;

        Type(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public boolean isDelete() {
            return this == Delete || this == DeleteColumn || this == DeleteFamily;
        }
    }

    /** Length fields of the binary layout: keylength + valuelength. */
    public static final int KEYVALUE_INFRASTRUCTURE_SIZE = 4 + 4;
    /** rowlength + familylength + timestamp + type. */
    public static final int KEY_INFRASTRUCTURE_SIZE = 2 + 1 + 8 + 1;

    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private final long timestamp;
    private final Type type;
    private final byte[] value;
    private long seqId;

    public KeyValue(byte[] row, byte[] family, byte[] qualifier, long timestamp, Type type, byte[] value) {
        this(row, family, qualifier, timestamp, type, value, 0);
    }

    public KeyValue(byte[] row, byte[] family, byte[] qualifier, long timestamp, Type type, byte[] value,
                    long seqId) {
        this.row = row;
        this.family = family == null ? Bytes.EMPTY_BYTE_ARRAY : family;
        this.qualifier = qualifier == null ? Bytes.EMPTY_BYTE_ARRAY : qualifier;
        this.timestamp = timestamp;
        this.type = type;
        this.value = value == null ? Bytes.EMPTY_BYTE_ARRAY : value;
        this.seqId = seqId;
    }

    public KeyValue(byte[] row, byte[] family, byte[] qualifier, long timestamp, byte[] value) {
        this(row, family, qualifier, timestamp, Type.Put, value);
    }

    // ---------------------------------------------------- keys for seeking
    /** The smallest possible key of a row - sorts before every real cell of the row. */
    public static KeyValue createFirstOnRow(byte[] row) {
        return new KeyValue(row, null, null, HConstants.LATEST_TIMESTAMP, Type.Maximum, null);
    }

    /** The largest possible key of a row - every real cell of the row sorts before it (see CellComparator). */
    public static KeyValue createLastOnRow(byte[] row) {
        return new KeyValue(row, null, null, HConstants.OLDEST_TIMESTAMP, Type.Minimum, null);
    }

    public static KeyValue createFirstOnRowCol(byte[] row, byte[] family, byte[] qualifier) {
        return new KeyValue(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Maximum, null);
    }

    /** Sorts after all versions of the column. */
    public static KeyValue createLastOnRowCol(byte[] row, byte[] family, byte[] qualifier) {
        return new KeyValue(row, family, qualifier, HConstants.OLDEST_TIMESTAMP, Type.Minimum, null);
    }

    // --------------------------------------------------------------- Cell
    @Override
    public byte[] getRow() {
        return row;
    }

    @Override
    public byte[] getFamily() {
        return family;
    }

    @Override
    public byte[] getQualifier() {
        return qualifier;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long getSequenceId() {
        return seqId;
    }

    public void setSequenceId(long seqId) {
        this.seqId = seqId;
    }

    /** Copy with a different timestamp (LATEST_TIMESTAMP is replaced by "now" on write). */
    public KeyValue withTimestamp(long ts) {
        return new KeyValue(row, family, qualifier, ts, type, value, seqId);
    }

    public int getKeyLength() {
        return KEY_INFRASTRUCTURE_SIZE + row.length + family.length + qualifier.length;
    }

    /** Size in the binary layout of the original (used for MemStore size, block size, statistics). */
    public int getSerializedSize() {
        return KEYVALUE_INFRASTRUCTURE_SIZE + getKeyLength() + value.length;
    }

    /** Key without value, as the original prints it: row/family:qualifier/timestamp/type. */
    public String keyToString() {
        return Bytes.toStringBinary(row) + "/" + Bytes.toStringBinary(family) + ":"
                + Bytes.toStringBinary(qualifier) + "/" + timestamp + "/" + type;
    }

    /** Same format as the original: row/family:qualifier/timestamp/type/vlen=N/seqid=S. */
    @Override
    public String toString() {
        return keyToString() + "/vlen=" + value.length + "/seqid=" + seqId;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Cell c && CellComparator.getInstance().compare(this, c) == 0;
    }

    @Override
    public int hashCode() {
        return Bytes.hashCode(row) * 31 + Long.hashCode(timestamp);
    }
}
