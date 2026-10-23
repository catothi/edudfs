package edubase.client;

import edubase.Cell;
import edubase.HConstants;
import edubase.KeyValue;
import edubase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Mutation: base class of Put and Delete.
 * A mutation always concerns exactly ONE row - and HBase guarantees atomicity per row:
 * all cells of a mutation become visible together or not at all.
 */
public abstract class Mutation implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final byte[] row;
    protected final long ts;
    protected final List<KeyValue> cells = new ArrayList<>();

    protected Mutation(byte[] row, long ts) {
        if (row == null || row.length == 0) {
            throw new IllegalArgumentException("Row key is empty");
        }
        this.row = row;
        this.ts = ts;
    }

    public byte[] getRow() {
        return row;
    }

    public long getTimestamp() {
        return ts;
    }

    /** All cells of the mutation (timestamps may still be LATEST_TIMESTAMP = "now"). */
    public List<KeyValue> getCells() {
        return Collections.unmodifiableList(cells);
    }

    /** Cells grouped by family (sorted by name) - each family goes to its own store. */
    public Map<String, List<Cell>> getFamilyCellMap() {
        Map<String, List<Cell>> m = new TreeMap<>();
        for (KeyValue kv : cells) {
            m.computeIfAbsent(Bytes.toString(kv.getFamily()), k -> new ArrayList<>()).add(kv);
        }
        return m;
    }

    public boolean isEmpty() {
        return cells.isEmpty();
    }

    protected static long orLatest(long ts) {
        return ts == HConstants.LATEST_TIMESTAMP ? HConstants.LATEST_TIMESTAMP : ts;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{row=" + Bytes.toStringBinary(row) + ", cells=" + cells + "}";
    }
}
