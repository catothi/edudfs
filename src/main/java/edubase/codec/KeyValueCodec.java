package edubase.codec;

import edubase.Cell;
import edubase.KeyValue;
import edubase.util.Bytes;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.codec.KeyValueCodec: how a cell is written to a file.
 *
 * <b>EduBase writes TEXT</b>, one line per cell - in exactly the format that the original
 * HFilePrettyPrinter prints with {@code -p}:
 * <pre>
 *   K: row1/cf:name/1761213640123/Put/vlen=5/seqid=7 V: Alice
 *      ^row ^family:qualifier ^timestamp ^type ^value length ^sequence ID      ^value
 * </pre>
 * Bytes that are not printable become {@code \xNN} (Bytes.toStringBinary). In the key parts
 * '/', ':' and ' ' are escaped as well ({@code \x2F}, {@code \x3A}, {@code \x20}), so the line can
 * be split unambiguously.
 *
 * <b>The original is binary:</b> in the HFile every cell is a KeyValue byte[] (see {@link KeyValue}),
 * optionally followed by the memstoreTS as a variable-length long; in the WAL the cells come after a
 * protobuf WALKey, also as KeyValue bytes (WALCellCodec). A text editor shows only fragments
 * of it.
 */
public final class KeyValueCodec {

    public static final String KEY_PREFIX = "K: ";
    public static final String VALUE_SEPARATOR = " V: ";
    private static final String KEY_ESCAPES = "/: ";

    private KeyValueCodec() {
    }

    /** row/family:qualifier/timestamp/type - the key of a cell (also used in the block index). */
    public static String encodeKey(Cell c) {
        return Bytes.toStringBinary(c.getRow(), KEY_ESCAPES) + "/" + Bytes.toStringBinary(c.getFamily(), KEY_ESCAPES)
                + ":" + Bytes.toStringBinary(c.getQualifier(), KEY_ESCAPES) + "/" + c.getTimestamp() + "/"
                + c.getType();
    }

    public static String encode(Cell c) {
        return KEY_PREFIX + encodeKey(c) + "/vlen=" + c.getValue().length + "/seqid=" + c.getSequenceId()
                + VALUE_SEPARATOR + Bytes.toStringBinary(c.getValue());
    }

    /** Parses a key from encodeKey() (without value and sequence ID). */
    public static KeyValue decodeKey(String key) throws IOException {
        String[] p = key.split("/", -1);
        if (p.length < 4) {
            throw new IOException("Corrupt key: " + key);
        }
        return toKeyValue(p, null, 0, key);
    }

    public static KeyValue decode(String line) throws IOException {
        if (!line.startsWith(KEY_PREFIX)) {
            throw new IOException("Not a cell line: " + line);
        }
        int v = line.indexOf(VALUE_SEPARATOR);
        if (v < 0) {
            throw new IOException("Cell line without value: " + line);
        }
        String[] p = line.substring(KEY_PREFIX.length(), v).split("/", -1);
        if (p.length != 6 || !p[4].startsWith("vlen=") || !p[5].startsWith("seqid=")) {
            throw new IOException("Corrupt cell line: " + line);
        }
        byte[] value = Bytes.toBytesBinary(line.substring(v + VALUE_SEPARATOR.length()));
        if (value.length != Integer.parseInt(p[4].substring(5))) {
            throw new IOException("Value length does not match vlen: " + line);
        }
        return toKeyValue(p, value, Long.parseLong(p[5].substring(6)), line);
    }

    private static KeyValue toKeyValue(String[] p, byte[] value, long seqId, String src) throws IOException {
        int colon = p[1].indexOf(':');
        if (colon < 0) {
            throw new IOException("Missing family:qualifier in " + src);
        }
        try {
            return new KeyValue(Bytes.toBytesBinary(p[0]), Bytes.toBytesBinary(p[1].substring(0, colon)),
                    Bytes.toBytesBinary(p[1].substring(colon + 1)), Long.parseLong(p[2]), KeyValue.Type.valueOf(p[3]),
                    value, seqId);
        } catch (IllegalArgumentException e) {
            throw new IOException("Corrupt key in " + src + ": " + e.getMessage());
        }
    }
}
