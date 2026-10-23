package edudfs.client;

import edudfs.common.ChecksumException;
import edudfs.common.DataChecksum;
import edudfs.common.Log;
import edudfs.protocol.ClientDatanodeProtocol;
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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hdfs.DFSInputStream.
 *
 * On open, fetches the block locations from the NameNode and then reads block by block
 * directly from a DataNode. If a DataNode fails or returns corrupt data
 * (checksum error), the next DataNode is tried; corrupt replicas are
 * reported to the NameNode (reportBadBlocks).
 *
 * Open file (still being written): the NameNode only counts the completed blocks. As in the original
 * (DFSInputStream.readBlockLength), the client asks a DataNode of the last block for its current
 * length (ClientDatanodeProtocol.getReplicaVisibleLength) and can then also read what has been
 * written - e.g. the WAL of a running HBase RegionServer.
 *
 * Besides sequential reading, the stream supports {@link #seek(long)} and positional reads
 * ("pread", {@link #read(long, byte[], int, int)}) as in the original (Seekable,
 * PositionedReadable). HBase needs them: an HFile reader jumps directly to the block that the
 * block index names.
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
    private final long fileLength;
    /** The last block is still being written (RBW) - do not report checksum errors for it. */
    private final LocatedBlock lastUcBlock;

    DFSInputStream(DFSClient dfsClient, String src) throws IOException {
        this.dfsClient = dfsClient;
        this.src = src;
        this.locatedBlocks = dfsClient.namenode.getBlockLocations(src, 0, Long.MAX_VALUE);
        List<LocatedBlock> blocks = locatedBlocks.getLocatedBlocks();
        LocatedBlock last = blocks.isEmpty() ? null : blocks.get(blocks.size() - 1);
        if (locatedBlocks.isUnderConstruction() && last != null
                && last.getStartOffset() >= locatedBlocks.getFileLength()) {
            last.getBlock().setNumBytes(readBlockLength(last));
            this.lastUcBlock = last;
            this.fileLength = last.getStartOffset() + last.getBlock().getNumBytes();
        } else {
            this.lastUcBlock = null;
            this.fileLength = locatedBlocks.getFileLength();
        }
    }

    /** Corresponds to DFSInputStream.readBlockLength: ask the DataNodes of the pipeline. */
    private long readBlockLength(LocatedBlock lb) {
        for (DatanodeInfo dn : lb.getLocations()) {
            try {
                ClientDatanodeProtocol cdp = (ClientDatanodeProtocol) LocateRegistry
                        .getRegistry(dn.getHostName(), dn.getIpcPort()).lookup(ClientDatanodeProtocol.NAME);
                return cdp.getReplicaVisibleLength(lb.getBlock());
            } catch (IOException | NotBoundException e) {
                Log.warn("DFSClient", "Cannot get visible length of " + lb.getBlock() + " from " + dn + ": "
                        + e.getMessage());
            }
        }
        return 0;  // no DataNode reachable: the open block stays invisible
    }

    public long getFileLength() {
        return fileLength;
    }

    /** Current position in the file (Seekable.getPos). */
    public synchronized long getPos() {
        return pos;
    }

    /**
     * Seekable.seek: the next read() starts at targetPos. The connection to the DataNode is
     * dropped; read() opens a new one at the right offset (blockSeekTo).
     */
    public synchronized void seek(long targetPos) throws IOException {
        if (targetPos < 0 || targetPos > getFileLength()) {
            throw new IOException("Cannot seek to " + targetPos + " in " + src + " (length " + getFileLength() + ")");
        }
        if (targetPos != pos) {
            closeBlockReader();
            pos = targetPos;
        }
    }

    /**
     * PositionedReadable.read ("pread"): reads at {@code position} without changing the
     * stream position. The original opens a separate connection for this, so several threads
     * can pread in parallel; here the stream is simply locked, repositioned and reset.
     */
    public synchronized int read(long position, byte[] b, int off, int len) throws IOException {
        long oldPos = pos;
        try {
            seek(position);
            return read(b, off, len);
        } finally {
            seek(oldPos);
        }
    }

    /** PositionedReadable.readFully: like pread, but reads exactly len bytes. */
    public synchronized void readFully(long position, byte[] b, int off, int len) throws IOException {
        int done = 0;
        while (done < len) {
            int n = read(position + done, b, off + done, len - done);
            if (n < 0) {
                throw new EOFException("End of file reached before reading fully: " + src + " at "
                        + (position + done));
            }
            done += n;
        }
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
                if (currentBlock == lastUcBlock) {
                    // the writer may just be rewriting the last partial chunk - not a corrupt replica
                    Log.warn("DFSClient", e.getMessage() + " in a block being written -> trying next DataNode");
                } else {
                    Log.warn("DFSClient", e.getMessage() + " -> reporting bad replica, trying next DataNode");
                    dfsClient.reportBadBlock(currentBlock, currentNode);
                }
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
