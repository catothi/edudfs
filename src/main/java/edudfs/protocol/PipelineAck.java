package edudfs.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck.
 * Acks travel backwards through the pipeline: each DataNode puts its own
 * status in front of the statuses of its successors. The client thus receives one status per DataNode.
 */
public class PipelineAck {

    public final long seqno;
    public final short[] replies;

    public PipelineAck(long seqno, short[] replies) {
        this.seqno = seqno;
        this.replies = replies;
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeLong(seqno);
        out.writeInt(replies.length);
        for (short r : replies) {
            out.writeShort(r);
        }
        out.flush();
    }

    public static PipelineAck read(DataInputStream in) throws IOException {
        long seqno = in.readLong();
        int n = in.readInt();
        if (n < 0 || n > 64) {
            throw new IOException("Invalid ack with " + n + " replies");
        }
        short[] replies = new short[n];
        for (int i = 0; i < n; i++) {
            replies[i] = in.readShort();
        }
        return new PipelineAck(seqno, replies);
    }
}
