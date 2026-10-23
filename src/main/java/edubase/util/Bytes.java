package edubase.util;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Corresponds to org.apache.hadoop.hbase.util.Bytes.
 *
 * HBase knows only byte[]: row keys, column families, qualifiers and values are all raw bytes,
 * compared lexicographically as UNSIGNED bytes. Strings and numbers are converted with this class.
 */
public final class Bytes {

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private Bytes() {
    }

    public static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(long v) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) v;
            v >>>= 8;
        }
        return b;
    }

    public static long toLong(byte[] b) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[i] & 0xff);
        }
        return v;
    }

    public static String toString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    /** Lexicographic comparison of unsigned bytes (original: via Unsafe, 8 bytes at a time). */
    public static int compareTo(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    public static boolean equals(byte[] a, byte[] b) {
        return Arrays.equals(a, b);
    }

    public static int hashCode(byte[] b) {
        return Arrays.hashCode(b);
    }

    public static byte[] add(byte[] a, byte[] b) {
        byte[] r = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    public static boolean isEmpty(byte[] b) {
        return b == null || b.length == 0;
    }

    public static String toHex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(HEX[(x >> 4) & 0xf]).append(HEX[x & 0xf]);
        }
        return sb.toString();
    }

    public static byte[] fromHex(String s) {
        byte[] b = new byte[s.length() / 2];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        }
        return b;
    }

    /**
     * Like the original: printable ASCII characters stay as they are, everything else becomes
     * {@code \xNN}. Used for all output (shell, logs, pretty printers).
     */
    public static String toStringBinary(byte[] b) {
        return toStringBinary(b, "");
    }

    /**
     * toStringBinary that additionally escapes the characters in {@code extra}. The text formats of
     * EduBase (HFile, WAL) need this for '/', ':' and ' ', which separate the fields of a key.
     */
    public static String toStringBinary(byte[] b, String extra) {
        if (b == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            int ch = x & 0xff;
            if (ch >= 32 && ch < 127 && ch != '\\' && extra.indexOf(ch) < 0) {
                sb.append((char) ch);
            } else {
                sb.append("\\x").append(HEX[ch >> 4]).append(HEX[ch & 0xf]);
            }
        }
        return sb.toString();
    }

    /** Inverse of toStringBinary: {@code \xNN} becomes the byte NN. */
    public static byte[] toBytesBinary(String s) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 3 < s.length() && s.charAt(i + 1) == 'x') {
                out.write(Integer.parseInt(s.substring(i + 2, i + 4), 16));
                i += 3;
            } else if (c < 128) {
                out.write(c);
            } else {
                out.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
            }
        }
        return out.toByteArray();
    }
}
