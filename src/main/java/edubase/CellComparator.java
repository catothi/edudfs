package edubase;

import edubase.util.Bytes;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Corresponds to org.apache.hadoop.hbase.CellComparatorImpl: THE sort order of HBase.
 * MemStore, HFile, block index and all scanners use it.
 *
 * <ol>
 *   <li>row ascending (unsigned bytes)</li>
 *   <li>family ascending, then qualifier ascending</li>
 *   <li>timestamp DESCENDING - the newest version comes first</li>
 *   <li>type DESCENDING by code - DeleteFamily(14) &gt; DeleteColumn(12) &gt; Delete(8) &gt; Put(4),
 *       so a delete marker comes before the Put it deletes</li>
 *   <li>sequence ID DESCENDING - with otherwise identical keys the later edit wins</li>
 * </ol>
 * Special rule (as in the original): a key with empty family and qualifier and type Minimum
 * ({@link KeyValue#createLastOnRow}) sorts AFTER all cells of its row.
 */
public final class CellComparator implements Comparator<Cell>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final CellComparator INSTANCE = new CellComparator();

    private CellComparator() {
    }

    public static CellComparator getInstance() {
        return INSTANCE;
    }

    @Override
    public int compare(Cell a, Cell b) {
        int c = compareRows(a.getRow(), b.getRow());
        if (c != 0) {
            return c;
        }
        c = compareWithoutRow(a, b);
        if (c != 0) {
            return c;
        }
        return Long.compare(b.getSequenceId(), a.getSequenceId());
    }

    public static int compareRows(byte[] a, byte[] b) {
        return Bytes.compareTo(a, b);
    }

    /** Compares only the key (row ... type) and ignores the sequence ID. */
    public int compareKeys(Cell a, Cell b) {
        int c = compareRows(a.getRow(), b.getRow());
        return c != 0 ? c : compareWithoutRow(a, b);
    }

    private static int compareWithoutRow(Cell a, Cell b) {
        if (isLastOnRow(a) && !isLastOnRow(b)) {
            return 1;
        }
        if (isLastOnRow(b) && !isLastOnRow(a)) {
            return -1;
        }
        int c = Bytes.compareTo(a.getFamily(), b.getFamily());
        if (c != 0) {
            return c;
        }
        c = Bytes.compareTo(a.getQualifier(), b.getQualifier());
        if (c != 0) {
            return c;
        }
        c = Long.compare(b.getTimestamp(), a.getTimestamp());
        if (c != 0) {
            return c;
        }
        return Integer.compare(b.getType().getCode(), a.getType().getCode());
    }

    private static boolean isLastOnRow(Cell c) {
        return c.getFamily().length == 0 && c.getQualifier().length == 0 && c.getType() == KeyValue.Type.Minimum;
    }

    public static boolean matchingRows(Cell a, Cell b) {
        return Bytes.equals(a.getRow(), b.getRow());
    }

    public static boolean matchingColumn(Cell a, Cell b) {
        return Bytes.equals(a.getFamily(), b.getFamily()) && Bytes.equals(a.getQualifier(), b.getQualifier());
    }
}
