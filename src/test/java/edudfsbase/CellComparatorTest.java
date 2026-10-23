package edudfsbase;

import edudfsbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The sort order of HBase: the basis of MemStore, HFile and every scanner. */
class CellComparatorTest {

    private static final CellComparator C = CellComparator.getInstance();
    private static final byte[] CF = Bytes.toBytes("cf");

    private static KeyValue kv(String row, String q, long ts, KeyValue.Type t, long seq) {
        return new KeyValue(Bytes.toBytes(row), CF, q == null ? null : Bytes.toBytes(q), ts, t, null, seq);
    }

    @Test
    void newestVersionFirstAndDeleteMarkersBeforePuts() {
        List<KeyValue> l = new ArrayList<>(List.of(
                kv("r", "a", 10, KeyValue.Type.Put, 1),
                kv("r", "a", 30, KeyValue.Type.Put, 3),
                kv("r", "a", 20, KeyValue.Type.DeleteColumn, 4),
                kv("r", "a", 20, KeyValue.Type.Put, 2),
                kv("r", null, 5, KeyValue.Type.DeleteFamily, 5),
                kv("q", "z", 1, KeyValue.Type.Put, 6)));
        Collections.sort(l, C);
        List<String> order = new ArrayList<>();
        for (KeyValue k : l) {
            order.add(Bytes.toString(k.getRow()) + "/" + Bytes.toString(k.getQualifier()) + "/" + k.getTimestamp() + "/"
                    + k.getType());
        }
        assertEquals(List.of("q/z/1/Put", "r//5/DeleteFamily", "r/a/30/Put", "r/a/20/DeleteColumn", "r/a/20/Put",
                "r/a/10/Put"), order);
    }

    @Test
    void laterEditWinsForIdenticalKeys() {
        assertTrue(C.compare(kv("r", "a", 1, KeyValue.Type.Put, 9), kv("r", "a", 1, KeyValue.Type.Put, 3)) < 0);
    }

    @Test
    void firstAndLastOnRowEncloseTheRow() {
        byte[] r = Bytes.toBytes("r");
        KeyValue first = KeyValue.createFirstOnRow(r);
        KeyValue last = KeyValue.createLastOnRow(r);
        KeyValue cell = kv("r", "zzz", Long.MIN_VALUE + 1, KeyValue.Type.Put, 0);
        KeyValue familyMarker = kv("r", null, 1, KeyValue.Type.DeleteFamily, 0);
        assertTrue(C.compare(first, familyMarker) < 0);
        assertTrue(C.compare(cell, last) < 0);
        assertTrue(C.compare(last, kv("r0", "a", 1, KeyValue.Type.Put, 0)) < 0);
    }

    @Test
    void rowsCompareAsUnsignedBytes() {
        assertTrue(CellComparator.compareRows(new byte[]{(byte) 0x7f}, new byte[]{(byte) 0x80}) < 0);
        assertTrue(CellComparator.compareRows(Bytes.toBytes("row10"), Bytes.toBytes("row9")) < 0);  // lexicographic!
    }
}
