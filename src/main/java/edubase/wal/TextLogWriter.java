package edubase.wal;

import edubase.Cell;
import edubase.codec.KeyValueCodec;
import edudfs.client.DFSOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Writes a WAL file. Corresponds to org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter -
 * but <b>EduBase writes text</b>:
 * <pre>
 *   PWAL hasCompression=false cellCodecClsName=edubase.codec.KeyValueCodec
 *   ENTRY encodedRegionName=3f2a…9c tableName=t logSeqNum=7 writeTime=1761213640123 cells=2
 *     K: row1/cf:name/1761213640123/Put/vlen=5/seqid=7 V: Alice
 *     K: row1/cf:age/1761213640123/Put/vlen=2/seqid=7 V: 42
 *   ...
 *   LAWP trailer
 * </pre>
 * <b>The original is binary:</b> magic "PWAL" (4 bytes), a protobuf WALHeader, then per entry a
 * length-prefixed protobuf WALKey followed by the cells as KeyValue bytes (WALCellCodec), and at
 * the end a protobuf WALTrailer + "LAWP". View it with {@code hbase wal} (WALPrettyPrinter).
 */
public class TextLogWriter implements Closeable {

    public static final String HEADER = "PWAL hasCompression=false cellCodecClsName=edubase.codec.KeyValueCodec";
    public static final String ENTRY = "ENTRY ";
    public static final String TRAILER = "LAWP trailer";

    private final DFSOutputStream out;
    private long length;
    private boolean closed;

    public TextLogWriter(DFSOutputStream out) throws IOException {
        this.out = out;
        write(HEADER + "\n");
    }

    private void write(String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.US_ASCII);
        out.write(b);
        length += b.length;
    }

    public void append(WAL.Entry entry) throws IOException {
        WALKeyImpl k = entry.getKey();
        StringBuilder sb = new StringBuilder();
        sb.append(ENTRY).append("encodedRegionName=").append(k.getEncodedRegionName()).append(" tableName=")
                .append(k.getTableName()).append(" logSeqNum=").append(k.getSequenceId()).append(" writeTime=")
                .append(k.getWriteTime()).append(" cells=").append(entry.getEdit().getCells().size()).append('\n');
        for (Cell c : entry.getEdit().getCells()) {
            sb.append("  ").append(KeyValueCodec.encode(c)).append('\n');
        }
        write(sb.toString());
    }

    /** hflush: the bytes are on all DataNodes of the pipeline. */
    public void sync() throws IOException {
        out.hflush();
    }

    public long getLength() {
        return length;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            write(TRAILER + "\n");
            out.close();
        }
    }
}
