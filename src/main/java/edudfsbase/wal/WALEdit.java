package edudfsbase.wal;

import edudfsbase.Cell;

import java.util.ArrayList;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.wal.WALEdit: the cells of ONE mutation (one row).
 * They are written as one WAL entry - so after a crash either all of them are replayed or none.
 */
public class WALEdit {

    private final List<Cell> cells;

    public WALEdit(List<? extends Cell> cells) {
        this.cells = new ArrayList<>(cells);
    }

    public List<Cell> getCells() {
        return cells;
    }

    public boolean isEmpty() {
        return cells.isEmpty();
    }
}
