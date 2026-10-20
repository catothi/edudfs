package edudfs.client;

import edudfs.common.ChecksumException;
import edudfs.common.DataChecksum;
import edudfs.common.Log;
import edudfs.protocol.DataTransferProtocol;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.LocatedBlock;
import edudfs.protocol.LocatedBlocks;
import edudfs.protocol.Packet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hdfs.DFSInputStream.
 *
 * On open, fetches the block locations from the NameNode and then reads block by block
 * directly from a DataNode. If a DataNode fails or returns corrupt data
 * (checksum error), the next DataNode is tried; corrupt replicas are
 * reported to the NameNode (reportBadBlocks).
 */
public class DFSInputStream extends InputStream {

    private final DFSClient dfsClient;
    private final String src;
    private final LocatedBlocks locatedBlocks;

    private long pos = 0;
    private BlockReader blockReader;
    private LocatedBlock currentBlock;
    private DatanodeInfo currentNode;
    private long blockEnd = -1;
    private final Set<DatanodeInfo> deadNodes = new HashSet<>();

    DFSInputStream(DFSClient dfsClient, String src) throws IOException {
        this.dfsClient = dfsClient;
        this.src = src;
        this.locatedBlocks = dfsClient.namenode.getBlockLocations(src, 0, Long.MAX_VALUE);
    }

    public long getFileLength() {
        return locatedBlocks.getFileLength();
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n <= 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (pos >= getFileLength()) {
            return -1;
        }
        while (true) {
            if (blockReader == null || pos > blockEnd) {
                blockSeekTo(pos);  // throws if no DataNode is left
            }
            try {
                int realLen = (int) Math.min(len, blockEnd - pos + 1);
                int n = blockReader.read(b, off, realLen);
                if (n < 0) {
                    throw new IOException("Unexpected end of block from " + currentNode);
                }
                pos += n;
                return n;
            } catch (ChecksumException e) {
                Log.warn("DFSClient", e.getMessage() + " -> reporting bad replica, trying next DataNode");
                dfsClient.reportBadBlock(currentBlock, currentNode);
                deadNodes.add(currentNode);
                closeBlockReader();
            } catch (IOException e) {
                Log.warn("DFSClient", "Failed to read " + currentBlock.getBlock() + " from " + currentNode + ": "
                        + e.getMessage() + " -> trying next DataNode");
                deadNodes.add(currentNode);
                closeBlockReader();
            }
        }
    }

    private void blockSeekTo(long target) throws IOException {
        closeBlockReader();
        LocatedBlock lb = null;
        for (LocatedBlock b : locatedBlocks.getLocatedBlocks()) {
            if (target >= b.getStartOffset() && target < b.getStartOffset() + b.getBlock().getNumBytes()) {
                lb = b;
            }
        }
        if (lb == null) {
            throw new IOException("No block found for offset " + target + " in " + src);
        }
        if (currentBlock == null || !currentBlock.getBlock().equals(lb.getBlock())) {
            deadNodes.clear();
        }
        currentBlock = lb;
        blockEnd = lb.getStartOffset() + lb.getBlock().getNumBytes() - 1;
        long offsetIntoBlock = target - lb.getStartOffset();
        for (DatanodeInfo dn : lb.getLocations()) {
            if (deadNodes.contains(dn)) {
                continue;
            }
            try {
                blockReader = new BlockReader(dn, lb, offsetIntoBlock);
                currentNode = dn;
                return;
            } catch (IOException e) {
                Log.warn("DFSClient", "Failed to connect to " + dn + " for " + lb.getBlock() + ": " + e.getMessage());
                deadNodes.add(dn);
            }
        }
        throw new IOException("Could not obtain block: " + lb.getBlock() + " file=" + src
                + " (no live replica left, tried " + deadNodes + ")");
    }

    private void closeBlockReader() {
        if (blockReader != null) {
            blockReader.close();
            blockReader = null;
        }
    }

    @Override
    public synchronized void close() {
        closeBlockReader();
    }

    /** Corresponds to BlockReaderRemote: reads the packets of a block and verifies the CRCs. */
    private static class BlockReader implements Closeable {
        private final Socket socket;
        private final DataInputStream in;
        private final String context;
        private final long startOffset;
        private byte[] cur = new byte[0];
        private int curPos;
        private int curLen;
        private boolean eos;

        BlockReader(DatanodeInfo dn, LocatedBlock lb, long startOffset) throws IOException {
            this.context = lb.getBlock() + " from " + dn;
            this.startOffset = startOffset;
            this.socket = DataTransferProtocol.connect(dn);
            try {
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 128 * 1024));
                DataTransferProtocol.sendReadBlock(out, lb.getBlock(), startOffset,
                        lb.getBlock().getNumBytes() - startOffset);
                short status = in.readShort();
                String msg = in.readUTF();
                if (status != DataTransferProtocol.SUCCESS) {
                    throw new IOException("Got error for OP_READ_BLOCK: " + msg);
                }
            } catch (IOException e) {
                socket.close();
                throw e;
            }
        }

        int read(byte[] b, int off, int len) throws IOException {
            while (curPos >= curLen) {
                if (eos) {
                    return -1;
                }
                readNextPacket();
            }
            int n = Math.min(len, curLen - curPos);
            System.arraycopy(cur, curPos, b, off, n);
            curPos += n;
            return n;
        }

        private void readNextPacket() throws IOException {
            Packet p = Packet.read(in);
            if (p.lastPacketInBlock && p.dataLen == 0) {
                eos = true;
                curPos = curLen = 0;
                return;
            }
            DataChecksum.verifyChunkedSums(p.data, 0, p.dataLen, p.checksums, context + " at offset "
                    + p.offsetInBlock);
            cur = p.data;
            curLen = p.dataLen;
            // The DataNode starts at the chunk boundary - skip the surplus bytes at the front
            curPos = p.offsetInBlock < startOffset ? (int) (startOffset - p.offsetInBlock) : 0;
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
    }
}
