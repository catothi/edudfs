package edubase.client;

import edubase.Cell;
import edubase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Result: the cells of ONE row, sorted
 * (family, qualifier, newest version first).
 *
 * EduBase extensions: {@link #getSources()} (where each cell came from) and {@link #getTrace()}
 * (read path, if requested with setTrace).
 */
public class Result implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Result EMPTY_RESULT = new Result(List.of(), null, null);

    private final List<Cell> cells;
    private final List<String> sources;
    private final List<String> trace;

    public Result(List<Cell> cells, List<String> sources, List<String> trace) {
        this.cells = new ArrayList<>(cells);
        this.sources = sources == null ? null : new ArrayList<>(sources);
        this.trace = trace == null ? null : new ArrayList<>(trace);
    }

    public byte[] getRow() {
        return cells.isEmpty() ? null : cells.get(0).getRow();
    }

    public Cell[] rawCells() {
        return cells.toArray(new Cell[0]);
    }

    public List<Cell> listCells() {
        return Collections.unmodifiableList(cells);
    }

    public boolean isEmpty() {
        return cells.isEmpty();
    }

    public int size() {
        return cells.size();
    }

    /** Value of the newest version of the column, null if not present. */
    public byte[] getValue(byte[] family, byte[] qualifier) {
        for (Cell c : cells) {
            if (Bytes.equals(c.getFamily(), family) && Bytes.equals(c.getQualifier(), qualifier)
                    && !c.getType().isDelete()) {
                return c.getValue();
            }
        }
        return null;
    }

    /** All versions of a column, newest first. */
    public List<Cell> getColumnCells(byte[] family, byte[] qualifier) {
        List<Cell> r = new ArrayList<>();
        for (Cell c : cells) {
            if (Bytes.equals(c.getFamily(), family) && Bytes.equals(c.getQualifier(), qualifier)) {
                r.add(c);
            }
        }
        return r;
    }

    /** Parallel to the cells: "MemStore", "MemStore snapshot" or "HFile &lt;name&gt;" (null if not requested). */
    public List<String> getSources() {
        return sources;
    }

    public List<String> getTrace() {
        return trace;
    }

    /** Same format as the original: keyvalues={row/cf:a/123/Put/vlen=5/seqid=7, ...}. */
    @Override
    public String toString() {
        return cells.isEmpty() ? "keyvalues=NONE" : "keyvalues=" + cells.toString().replace('[', '{').replace(']', '}');
    }
}
