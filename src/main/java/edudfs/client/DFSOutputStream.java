package edudfs.client;

import edudfs.common.DataChecksum;
import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.protocol.Block;
import edudfs.protocol.DataTransferProtocol;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LocatedBlock;
import edudfs.protocol.Packet;
import edudfs.protocol.PipelineAck;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.DFSOutputStream (+ DataStreamer).
 *
 * Write flow:
 * <pre>
 *  write() --&gt; packet (64 KB, with a CRC per 512 B) --&gt; dataQueue
 *  DataStreamer thread: addBlock() at the NN, set up pipeline, send packets --&gt; ackQueue
 *  ResponseProcessor thread: read acks, remove packets from ackQueue
 *  End of block: empty packet with lastPacketInBlock, wait for all acks, next block
 *  close(): finish the last block, complete() at the NN (with retry)
 * </pre>
 * Simplifications: no pipeline recovery on DataNode failure during the write
 * (the stream fails), no hflush/hsync; append always starts a new block
 * (like CreateFlag.NEW_BLOCK in the original), a last block that is not full is not continued.
 */
public class DFSOutputStream extends OutputStream {

    private final DFSClient dfsClient;
    private final String src;
    private final long fileId;
    private final long blockSize;

    private final byte[] buf = new byte[HdfsConfig.PACKET_SIZE];
    private int bufCount = 0;
    /** Bytes the user has written into the current block (including the buffer). */
    private long bytesCurBlock = 0;
    private long seqno = 0;
    private boolean closed = false;

    private final DataStreamer streamer;

    DFSOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat) {
        this(dfsClient, src, stat, null);
    }

    /**
     * @param lastBlock on append: the file's current last (completed) block. It is passed as
     *                  "previous" on the first addBlock() so that the NameNode appends the new
     *                  block after it.
     */
    DFSOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat, Block lastBlock) {
        this.dfsClient = dfsClient;
        this.src = src;
        this.fileId = stat.getFileId();
        this.blockSize = stat.getBlockSize();
        this.streamer = new DataStreamer(lastBlock);
        streamer.start();
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed: " + src);
        }
        streamer.checkError();
        while (len > 0) {
            // A packet must not cross a block boundary
            long packetStart = bytesCurBlock - bufCount;
            int capacity = (int) Math.min(HdfsConfig.PACKET_SIZE, blockSize - packetStart);
            int n = Math.min(len, capacity - bufCount);
            System.arraycopy(b, off, buf, bufCount, n);
            bufCount += n;
            bytesCurBlock += n;
            off += n;
            len -= n;
            if (bufCount == capacity) {
                enqueueCurrentPacket();
            }
            if (bytesCurBlock == blockSize) {
                streamer.enqueue(Packet.lastPacket(bytesCurBlock, seqno++));
                bytesCurBlock = 0;
            }
        }
    }

    private void enqueueCurrentPacket() throws IOException {
        long offset = bytesCurBlock - bufCount;
        byte[] data = Arrays.copyOf(buf, bufCount);
        int[] sums = DataChecksum.computeChunkedSums(data, 0, bufCount);
        streamer.enqueue(new Packet(offset, seqno++, false, data, bufCount, sums));
        bufCount = 0;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            if (bufCount > 0) {
                enqueueCurrentPacket();
            }
            if (bytesCurBlock > 0) {
                streamer.enqueue(Packet.lastPacket(bytesCurBlock, seqno++));
                bytesCurBlock = 0;
            }
            streamer.flushAndStop();
            completeFile(streamer.getLastBlock());
        } finally {
            closed = true;
            streamer.closeStream();
            dfsClient.endFileLease(fileId);
        }
    }

    /** Retry complete() until the NN records all blocks as COMPLETE. */
    private void completeFile(Block last) throws IOException {
        long sleeptime = 100;
        for (int retries = 10; ; retries--) {
            if (dfsClient.namenode.complete(src, dfsClient.clientName, last, fileId)) {
                return;
            }
            if (retries == 0) {
                throw new IOException("Unable to close file because the last block " + last
                        + " does not have enough number of replicas.");
            }
            Log.info("DFSClient", "Waiting for NameNode to complete " + src + " (retry in " + sleeptime + " ms)");
            try {
                Thread.sleep(sleeptime);
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
            sleeptime = Math.min(sleeptime * 2, 2000);
        }
    }

    /** Hard abort without complete() (client crash). */
    void abort() {
        synchronized (this) {
            closed = true;
        }
        streamer.abort();
    }

    // =========================================================================
    /** Corresponds to org.apache.hadoop.hdfs.DataStreamer. */
    private class DataStreamer extends Thread {

        private final LinkedList<Packet> dataQueue = new LinkedList<>();
        private final LinkedList<Packet> ackQueue = new LinkedList<>();  // sent, not yet acknowledged
        private volatile boolean streamerClosed = false;
        private volatile boolean closing = false;
        private volatile IOException lastException;

        private Block block;      // block currently being written
        private Block lastBlock;  // most recently completed block (= "previous" for addBlock)
        private DatanodeInfo[] nodes;
        private Socket socket;
        private DataOutputStream blockStream;
        private DataInputStream blockReplyStream;
        private final List<DatanodeInfo> excludedNodes = new ArrayList<>();

        DataStreamer(Block lastBlock) {
            super("DataStreamer for " + src);
            this.lastBlock = lastBlock;
            setDaemon(true);
        }

        void checkError() throws IOException {
            IOException e = lastException;
            if (e != null) {
                throw new IOException("Write to " + src + " failed: " + e.getMessage(), e);
            }
        }

        private void setLastException(IOException e) {
            synchronized (dataQueue) {
                if (lastException == null) {
                    lastException = e;
                }
                dataQueue.notifyAll();
            }
        }

        void enqueue(Packet p) throws IOException {
            synchronized (dataQueue) {
                while (lastException == null && !streamerClosed
                        && dataQueue.size() + ackQueue.size() >= HdfsConfig.MAX_PACKETS_IN_FLIGHT) {
                    try {
                        dataQueue.wait();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
                checkError();
                dataQueue.addLast(p);
                dataQueue.notifyAll();
            }
        }

        Block getLastBlock() {
            return lastBlock == null ? null : new Block(lastBlock);
        }

        /** Let the remaining packets be sent and wait for the thread to finish. */
        void flushAndStop() throws IOException {
            synchronized (dataQueue) {
                closing = true;
                dataQueue.notifyAll();
            }
            try {
                join();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
            checkError();
        }

        void abort() {
            streamerClosed = true;
            closeStream();
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Packet one;
                    synchronized (dataQueue) {
                        while (!streamerClosed && lastException == null && dataQueue.isEmpty() && !closing) {
                            dataQueue.wait(1000);
                        }
                        if (streamerClosed || lastException != null || dataQueue.isEmpty()) {
                            return;
                        }
                        one = dataQueue.getFirst();
                    }
                    if (blockStream == null) {
                        nextBlockOutputStream();
                    }
                    synchronized (dataQueue) {
                        dataQueue.removeFirst();
                        ackQueue.addLast(one);
                        dataQueue.notifyAll();
                    }
                    one.write(blockStream);
                    blockStream.flush();
                    if (one.dataLen > 0) {
                        block.setNumBytes(one.offsetInBlock + one.dataLen);
                    }
                    if (one.lastPacketInBlock) {
                        // End of block: wait until all DataNodes have acknowledged all packets
                        synchronized (dataQueue) {
                            while (!streamerClosed && lastException == null && !ackQueue.isEmpty()) {
                                dataQueue.wait(1000);
                            }
                        }
                        checkError();
                        endBlock();
                    }
                }
            } catch (IOException e) {
                if (!streamerClosed) {
                    setLastException(e);
                }
            } catch (InterruptedException e) {
                if (!streamerClosed) {
                    setLastException(new InterruptedIOException("DataStreamer interrupted"));
                }
            } catch (RuntimeException e) {
                if (!streamerClosed) {  // after abort() the streams may already be null
                    setLastException(new IOException(e));
                }
            } finally {
                closeStream();
            }
        }

        private void endBlock() {
            closeStream();
            Log.info("DFSClient", "Finished writing " + block + " (" + block.getNumBytes() + " bytes) of " + src);
            lastBlock = block;
            block = null;
            nodes = null;
        }

        /** Get a new block from the NameNode and set up the pipeline (on error: abandon + exclude). */
        private void nextBlockOutputStream() throws IOException {
            for (int retries = 3; ; retries--) {
                LocatedBlock lb = dfsClient.namenode.addBlock(src, dfsClient.clientName, getLastBlock(), fileId,
                        excludedNodes.toArray(new DatanodeInfo[0]));
                block = new Block(lb.getBlock());
                nodes = lb.getLocations();
                try {
                    createBlockOutputStream();
                    return;
                } catch (BadNodeException e) {
                    Log.warn("DFSClient", "Pipeline setup for " + block + " failed (" + e.getMessage()
                            + "), abandoning block and excluding " + e.badNode);
                    dfsClient.namenode.abandonBlock(block, fileId, src, dfsClient.clientName);
                    excludedNodes.add(e.badNode);
                    block = null;
                    if (retries <= 1) {
                        throw new IOException("Unable to create new block for " + src, e);
                    }
                }
            }
        }

        private void createBlockOutputStream() throws BadNodeException {
            Socket s = null;
            try {
                s = DataTransferProtocol.connect(nodes[0]);
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 128 * 1024));
                DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                DataTransferProtocol.sendWriteBlock(out, block, dfsClient.clientName,
                        Arrays.copyOfRange(nodes, 1, nodes.length));
                short status = in.readShort();
                String firstBadLink = in.readUTF();
                if (status != DataTransferProtocol.SUCCESS) {
                    DatanodeInfo bad = nodes[0];
                    for (DatanodeInfo n : nodes) {
                        if (n.getXferAddr().equals(firstBadLink)) {
                            bad = n;
                        }
                    }
                    throw new BadNodeException(bad, "Bad connect ack with firstBadLink as " + firstBadLink);
                }
                socket = s;
                blockStream = out;
                blockReplyStream = in;
                Thread rp = new Thread(new ResponseProcessor(nodes, in), "ResponseProcessor for " + block);
                rp.setDaemon(true);
                rp.start();
                Log.info("DFSClient", "Writing " + block + " through pipeline " + Arrays.toString(nodes));
            } catch (BadNodeException e) {
                closeQuietly(s);
                throw e;
            } catch (IOException e) {
                closeQuietly(s);
                throw new BadNodeException(nodes[0], e.getMessage());
            }
        }

        void closeStream() {
            closeQuietly(socket);
            socket = null;
            blockStream = null;
            blockReplyStream = null;
        }

        private void closeQuietly(Socket s) {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // ignore
                }
            }
        }

        /** Reads the pipeline's acks (a separate thread per block). */
        private class ResponseProcessor implements Runnable {
            private final DatanodeInfo[] targets;
            private final DataInputStream in;

            ResponseProcessor(DatanodeInfo[] targets, DataInputStream in) {
                this.targets = targets;
                this.in = in;
            }

            @Override
            public void run() {
                try {
                    while (!streamerClosed) {
                        PipelineAck ack = PipelineAck.read(in);
                        for (int i = 0; i < ack.replies.length; i++) {
                            if (ack.replies[i] != DataTransferProtocol.SUCCESS) {
                                throw new IOException("Bad response " + ack.replies[i] + " from " + targets[i]);
                            }
                        }
                        Packet one;
                        synchronized (dataQueue) {
                            one = ackQueue.peekFirst();
                            if (one == null || one.seqno != ack.seqno) {
                                throw new IOException("Unexpected ack seqno " + ack.seqno);
                            }
                            ackQueue.removeFirst();
                            dataQueue.notifyAll();
                        }
                        if (one.lastPacketInBlock) {
                            return;
                        }
                    }
                } catch (IOException e) {
                    if (!streamerClosed) {
                        setLastException(e);
                    }
                }
            }
        }
    }

    private static class BadNodeException extends IOException {
        private static final long serialVersionUID = 1L;
        final transient DatanodeInfo badNode;

        BadNodeException(DatanodeInfo badNode, String msg) {
            super(msg);
            this.badNode = badNode;
        }
    }
}
