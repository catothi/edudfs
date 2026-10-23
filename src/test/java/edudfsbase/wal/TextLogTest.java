package edudfsbase.wal;

import edudfsbase.KeyValue;
import edudfsbase.TableName;
import edudfsbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** WAL text format: round trip, trailer, and an entry cut off by a crash. */
class TextLogTest {

    private static WAL.Entry entry(long seq, String row, String... values) {
        KeyValue[] cells = new KeyValue[values.length];
        for (int i = 0; i < values.length; i++) {
            cells[i] = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("cf"), Bytes.toBytes("q" + i), 1000 + seq,
                    KeyValue.Type.Put, Bytes.toBytes(values[i]), seq);
        }
        return new WAL.Entry(new WALKeyImpl("abcdef0123456789", TableName.valueOf("t"), seq, 5000 + seq),
                new WALEdit(List.of(cells)));
    }

    private static byte[] write(boolean close, WAL.Entry... entries) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        TextLogWriter w = new TextLogWriter(bos);
        for (WAL.Entry e : entries) {
            w.append(e);
        }
        w.sync();
        if (close) {
            w.close();
        }
        return bos.toByteArray();
    }

    @Test
    void roundTripWithTrailer() throws IOException {
        byte[] wal = write(true, entry(1, "r1", "a", "b"), entry(2, "r2", "with space and \\ backslash"));
        String text = new String(wal, StandardCharsets.US_ASCII);
        assertTrue(text.startsWith("PWAL "));
        assertTrue(text.endsWith("LAWP trailer\n"));
        TextLogReader r = new TextLogReader(new ByteArrayInputStream(wal), "test");
        WAL.Entry e1 = r.next();
        assertEquals(1, e1.getKey().getSequenceId());
        assertEquals(2, e1.getEdit().getCells().size());
        WAL.Entry e2 = r.next();
        assertEquals("with space and \\ backslash", Bytes.toString(e2.getEdit().getCells().get(0).getValue()));
        assertNull(r.next());
        assertTrue(r.hasTrailer());
        assertFalse(r.isTruncated());
    }

    @Test
    void entryCutOffByCrashIsIgnored() throws IOException {
        byte[] wal = write(false, entry(1, "r1", "a"), entry(2, "r2", "complete"), entry(3, "r3", "x", "y"));
        byte[] cut = Arrays.copyOf(wal, wal.length - 10);  // the last cell line is incomplete
        TextLogReader r = new TextLogReader(new ByteArrayInputStream(cut), "test");
        assertEquals(1, r.next().getKey().getSequenceId());
        assertEquals(2, r.next().getKey().getSequenceId());
        assertNull(r.next());  // entry 3 is not replayed - it was never acknowledged
        assertTrue(r.isTruncated());
        assertFalse(r.hasTrailer());
    }
}
