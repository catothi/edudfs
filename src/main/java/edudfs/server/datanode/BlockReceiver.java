package edudfs.server.datanode;

import edudfs.common.DataChecksum;
import edudfs.common.Log;
import edudfs.protocol.DataTransferProtocol;
import edudfs.protocol.Packet;
import edudfs.protocol.PipelineAck;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.BlockReceiver.
 *
 * Receives the packets of a block and
 * <ol>
 *   <li>immediately forwards each packet to the next DataNode ("mirror"),</li>
 *   <li>verifies the checksums (only the LAST DataNode of the pipeline, as in the original),</li>
 *   <li>writes data + CRCs to disk,</li>
 *   <li>hands the packet to the PacketResponder, which sends the acks (own status +
 *       status of the downstream nodes) upstream.</li>
 * </ol>
 */
class BlockReceiver implements Closeable {

    private final DataNode datanode;
    private final ReplicaInfo replica;
    private final Socket upstreamSocket;
    private final DataInputStream in;
    private final DataOutputStream replyOut;
    private final DataOutputStream mirrorOut;
    private final DataInputStream mirrorIn;
    private final FileOutputStream dataOut;
    private final DataOutputStream checksumOut;

    private final BlockingQueue<Packet> ackQueue = new LinkedBlockingQueue<>();
    private volatile IOException responderError;

    BlockReceiver(DataNode datanode, ReplicaInfo replica, Socket upstreamSocket, DataInputStream in,
                  DataOutputStream replyOut, DataOutputStream mirrorOut, DataInputStream mirrorIn) throws IOException {
        this.datanode = datanode;
        this.replica = replica;
        this.upstreamSocket = upstreamSocket;
        this.in = in;
        this.replyOut = replyOut;
        this.mirrorOut = mirrorOut;
        this.mirrorIn = mirrorIn;
        FsDatasetImpl data = datanode.getFSDataset();
        this.dataOut = new FileOutputStream(data.getBlockFile(replica), true);
        this.checksumOut = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(data.getMetaFile(replica), true)));
    }

    void receiveBlock() throws IOException {
        Thread responder = new Thread(new PacketResponder(), "PacketResponder: " + replica.toBlock());
        responder.setDaemon(true);
        responder.start();
        try {
            while (true) {
                Packet p = Packet.read(in);
                if (p.offsetInBlock != replica.getNumBytes()) {
                    throw new IOException("Unexpected " + p + ", replica has " + replica.getNumBytes() + " bytes");
                }
                // 1) forward immediately (pipelining!)
                if (mirrorOut != null) {
                    p.write(mirrorOut);
                    mirrorOut.flush();
                } else if (p.dataLen > 0) {
                    // 2) the last DataNode verifies the checksums
                    DataChecksum.verifyChunkedSums(p.data, 0, p.dataLen, p.checksums,
                            replica.toBlock() + " " + p + " at " + datanode);
                }
                // 3) write to disk
                if (p.dataLen > 0) {
                    dataOut.write(p.data, 0, p.dataLen);
                    for (int c : p.checksums) {
                        checksumOut.writeInt(c);
                    }
                    checksumOut.flush();
                    replica.setNumBytes(p.offsetInBlock + p.dataLen);
                }
                if (p.lastPacketInBlock) {
                    close();
                    datanode.getFSDataset().finalizeBlock(replica);
                    Log.info("DataNode", datanode + ": received and finalized " + replica);
                    datanode.notifyNamenodeReceivedBlock(replica);
                }
                // 4) enqueue for ack
                ackQueue.put(p);
                if (p.lastPacketInBlock) {
                    break;
                }
            }
            responder.join();
            if (responderError != null) {
                throw responderError;
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException("Interrupted while receiving " + replica);
        } finally {
            close();
            responder.interrupt();
        }
    }

    @Override
    public void close() {
        try {
            dataOut.close();
        } catch (IOException ignored) {
            // ignore
        }
        try {
            checksumOut.close();
        } catch (IOException ignored) {
            // ignore
        }
    }

    /** Sends the acks upstream after the downstream node has acknowledged in turn. */
    private class PacketResponder implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    Packet p = ackQueue.take();
                    short[] downstream = new short[0];
                    if (mirrorIn != null) {
                        PipelineAck ack = PipelineAck.read(mirrorIn);
                        if (ack.seqno != p.seqno) {
                            throw new IOException("Expected ack for seqno " + p.seqno + " but got " + ack.seqno);
                        }
                        downstream = ack.replies;
                    }
                    short[] replies = new short[downstream.length + 1];
                    replies[0] = DataTransferProtocol.SUCCESS;
                    System.arraycopy(downstream, 0, replies, 1, downstream.length);
                    new PipelineAck(p.seqno, replies).write(replyOut);
                    if (p.lastPacketInBlock) {
                        return;
                    }
                }
            } catch (InterruptedException e) {
                // receiving was aborted
            } catch (IOException e) {
                responderError = e;
                try {
                    upstreamSocket.close();  // tear down the pipeline -> the receiving thread aborts
                } catch (IOException ignored) {
                    // ignore
                }
            }
        }
    }
}
