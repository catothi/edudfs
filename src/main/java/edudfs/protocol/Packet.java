package edudfs.protocol;

import edudfs.common.DataChecksum;
import edudfs.common.HdfsConfig;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Corresponds to DFSPacket / PacketHeader / PacketReceiver.
 *
 * <pre>
 * long offsetInBlock | long seqno | boolean lastPacketInBlock | int dataLen
 * int numChecksums   | int[numChecksums] CRC32 (per 512 bytes) | byte[dataLen] data
 * </pre>
 * A block is always terminated by an empty packet with lastPacketInBlock=true.
 * A heartbeat packet (seqno = -1, no data) keeps an idle pipeline alive: the DataNodes forward and
 * acknowledge it, but write nothing.
 */
public class Packet {

    public final long offsetInBlock;
    public final long seqno;
    public final boolean lastPacketInBlock;
    public final byte[] data;
    public final int dataLen;
    public final int[] checksums;

    public Packet(long offsetInBlock, long seqno, boolean lastPacketInBlock, byte[] data, int dataLen,
                  int[] checksums) {
        this.offsetInBlock = offsetInBlock;
        this.seqno = seqno;
        this.lastPacketInBlock = lastPacketInBlock;
        this.data = data;
        this.dataLen = dataLen;
        this.checksums = checksums;
    }

    /** PacketHeader.HEART_BEAT_SEQNO in the original. */
    public static final long HEART_BEAT_SEQNO = -1;

    public static Packet heartbeat() {
        return new Packet(0, HEART_BEAT_SEQNO, false, new byte[0], 0, new int[0]);
    }

    public boolean isHeartbeatPacket() {
        return seqno == HEART_BEAT_SEQNO;
    }

    public static Packet lastPacket(long offsetInBlock, long seqno) {
        return new Packet(offsetInBlock, seqno, true, new byte[0], 0, new int[0]);
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeLong(offsetInBlock);
        out.writeLong(seqno);
        out.writeBoolean(lastPacketInBlock);
        out.writeInt(dataLen);
        out.writeInt(checksums.length);
        for (int c : checksums) {
            out.writeInt(c);
        }
        out.write(data, 0, dataLen);
    }

    public static Packet read(DataInputStream in) throws IOException {
        long offset = in.readLong();
        long seqno = in.readLong();
        boolean last = in.readBoolean();
        int dataLen = in.readInt();
        int numSums = in.readInt();
        if (dataLen < 0 || dataLen > HdfsConfig.PACKET_SIZE || numSums != DataChecksum.numChunks(dataLen)) {
            throw new IOException("Invalid packet header: dataLen=" + dataLen + ", checksums=" + numSums);
        }
        int[] sums = new int[numSums];
        for (int i = 0; i < numSums; i++) {
            sums[i] = in.readInt();
        }
        byte[] data = new byte[dataLen];
        in.readFully(data);
        return new Packet(offset, seqno, last, data, dataLen, sums);
    }

    @Override
    public String toString() {
        return "packet(seqno=" + seqno + ", offsetInBlock=" + offsetInBlock + ", len=" + dataLen
                + (lastPacketInBlock ? ", lastPacketInBlock" : "") + ")";
    }
}
