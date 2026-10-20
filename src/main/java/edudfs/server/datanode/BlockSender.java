package edudfs.server.datanode;

import edudfs.common.DataChecksum;
import edudfs.common.HdfsConfig;
import edudfs.protocol.Block;
import edudfs.protocol.Packet;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.BlockSender.
 * Reads data + stored CRCs from disk and sends them as packets -
 * both to reading clients and to other DataNodes during re-replication.
 * The range is extended to chunk boundaries (512 bytes) so that the CRCs match.
 */
class BlockSender implements Closeable {

    private final RandomAccessFile blockIn;
    private final RandomAccessFile metaIn;
    private final Block block;
    private final long startOffset;
    private final long endOffset;
    private long lastSeqno;

    BlockSender(FsDatasetImpl data, Block block, long offset, long length) throws IOException {
        int bpc = HdfsConfig.BYTES_PER_CHECKSUM;
        ReplicaInfo r;
        synchronized (data) {
            r = data.getReplica(block.getBlockId());
            if (r == null) {
                throw new IOException("Replica not found for " + block);
            }
            if (r.getGenerationStamp() < block.getGenerationStamp()) {
                throw new IOException("Replica " + r + " is older than requested " + block);
            }
            this.blockIn = new RandomAccessFile(data.getBlockFile(r), "r");
            this.metaIn = new RandomAccessFile(data.getMetaFile(r), "r");
        }
        this.block = r.toBlock();
        long replicaLen = r.getNumBytes();
        if (offset < 0 || offset > replicaLen) {
            close();
            throw new IOException("Offset " + offset + " out of range for " + r);
        }
        long end = length >= replicaLen - offset ? replicaLen : offset + length;
        this.startOffset = offset - offset % bpc;
        this.endOffset = Math.min(replicaLen, (end + bpc - 1) / bpc * bpc);
    }

    Block getBlock() {
        return block;
    }

    long getLastSeqno() {
        return lastSeqno;
    }

    /** Sends all packets and finishes with an empty lastPacketInBlock packet. */
    void sendBlock(DataOutputStream out) throws IOException {
        int bpc = HdfsConfig.BYTES_PER_CHECKSUM;
        byte[] buf = new byte[HdfsConfig.PACKET_SIZE];
        long offset = startOffset;
        long seqno = 0;
        while (offset < endOffset) {
            int len = (int) Math.min(HdfsConfig.PACKET_SIZE, endOffset - offset);
            blockIn.seek(offset);
            blockIn.readFully(buf, 0, len);
            int[] sums = new int[DataChecksum.numChunks(len)];
            metaIn.seek(FsDatasetImpl.META_HEADER_SIZE + (offset / bpc) * DataChecksum.CHECKSUM_SIZE);
            for (int i = 0; i < sums.length; i++) {
                sums[i] = metaIn.readInt();
            }
            new Packet(offset, seqno++, false, buf, len, sums).write(out);
            offset += len;
        }
        Packet.lastPacket(offset, seqno).write(out);
        lastSeqno = seqno;
        out.flush();
    }

    @Override
    public void close() throws IOException {
        blockIn.close();
        metaIn.close();
    }
}
