package edudfsbase.wal;

import edudfsbase.KeyValue;
import edudfsbase.TableName;
import edudfsbase.codec.KeyValueCodec;
import edudfs.common.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a WAL file written by {@link TextLogWriter}. Corresponds to ProtobufLogReader.
 *
 * A WAL whose writer crashed ends without "LAWP" - possibly in the middle of an entry (only part of
 * it reached the DataNodes before the crash; it was then never acknowledged to the client either).
 * As in the original, such a cut-off last entry is ignored: everything before it is complete.
 */
public class TextLogReader implements WAL.Reader {

    private final BufferedReader in;
    private final String name;
    private boolean eof;
    private boolean truncated;
    private boolean sawTrailer;

    public TextLogReader(InputStream in, String name) throws IOException {
        this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII));
        this.name = name;
        String header = this.in.readLine();
        if (header == null) {
            eof = true;  // empty file: the writer crashed right after creating it
        } else if (!header.startsWith("PWAL")) {
            throw new IOException("Not a WAL file (no PWAL magic): " + name);
        }
    }

    @Override
    public WAL.Entry next() throws IOException {
        if (eof) {
            return null;
        }
        String line = in.readLine();
        if (line == null || line.equals(TextLogWriter.TRAILER)) {
            eof = true;
            sawTrailer = line != null;
            return null;
        }
        try {
            if (!line.startsWith(TextLogWriter.ENTRY)) {
                throw new IOException("bad entry header");
            }
            Map<String, String> f = new HashMap<>();
            for (String kv : line.substring(TextLogWriter.ENTRY.length()).split(" ")) {
                int i = kv.indexOf('=');
                f.put(kv.substring(0, i), kv.substring(i + 1));
            }
            int n = Integer.parseInt(f.get("cells"));
            List<KeyValue> cells = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                String c = in.readLine();
                if (c == null) {
                    throw new IOException("entry cut off after " + i + " of " + n + " cells");
                }
                cells.add(KeyValueCodec.decode(c.trim()));
            }
            WALKeyImpl key = new WALKeyImpl(f.get("encodedRegionName"), TableName.valueOf(f.get("tableName")),
                    Long.parseLong(f.get("logSeqNum")), Long.parseLong(f.get("writeTime")));
            return new WAL.Entry(key, new WALEdit(cells));
        } catch (IOException | RuntimeException e) {
            eof = true;
            truncated = true;
            Log.warn("TextLogReader", "Truncated last entry in " + name + " (" + e.getMessage()
                    + ") - ignored, as it was never acknowledged");
            return null;
        }
    }

    /** true if the file ended with a cut-off entry. */
    public boolean isTruncated() {
        return truncated;
    }

    /** true if the file was closed properly ("LAWP" trailer). */
    public boolean hasTrailer() {
        return sawTrailer;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
