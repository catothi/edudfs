package edudfs.server.datanode;

import edudfs.common.Log;
import edudfs.protocol.Block;
import edudfs.protocol.DataTransferProtocol;
import edudfs.protocol.DatanodeInfo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.DataXceiver:
 * handles ONE data connection (one thread per connection).
 */
class DataXceiver implements Runnable {

    private static final String LOG = "DataXceiver";

    private final DataNode datanode;
    private final Socket socket;

    DataXceiver(DataNode datanode, Socket socket) {
        this.datanode = datanode;
        this.socket = socket;
    }

    @Override
    public void run() {
        try (Socket s = socket) {
            s.setSoTimeout(DataTransferProtocol.READ_TIMEOUT_MS);
            s.setTcpNoDelay(true);
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 128 * 1024));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 128 * 1024));
            short version = in.readShort();
            if (version != DataTransferProtocol.DATA_TRANSFER_VERSION) {
                throw new IOException("Version mismatch: " + version);
            }
            byte op = in.readByte();
            switch (op) {
                case DataTransferProtocol.OP_WRITE_BLOCK:
                    writeBlock(in, out);
                    break;
                case DataTransferProtocol.OP_READ_BLOCK:
                    readBlock(in, out);
                    break;
                default:
                    throw new IOException("Unknown op " + op);
            }
        } catch (EOFException e) {
            Log.info(LOG, datanode + ": connection closed by peer (" + e.getClass().getSimpleName() + ")");
        } catch (IOException e) {
            if (datanode.isRunning()) {
                Log.info(LOG, datanode + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        } finally {
            datanode.getXceiverServer().remove(socket);
        }
    }

    /** OP_WRITE_BLOCK: create the RBW, set up the pipeline to the downstream node, receive packets. */
    private void writeBlock(DataInputStream in, DataOutputStream out) throws IOException {
        Block block = DataTransferProtocol.readBlock(in);
        String clientName = in.readUTF();
        int n = in.readInt();
        DatanodeInfo[] targets = new DatanodeInfo[n];
        for (int i = 0; i < n; i++) {
            targets[i] = DataTransferProtocol.readDatanodeInfo(in);
        }
        Log.info(LOG, datanode + ": receiving " + block + " from " + (clientName.isEmpty() ? "datanode" : clientName)
                + ", downstream=" + Arrays.toString(targets));

        ReplicaInfo replica = datanode.getFSDataset().createRbw(block);
        replica.setWriter(Thread.currentThread(), socket);
        Socket mirrorSock = null;
        try {
            DataOutputStream mirrorOut = null;
            DataInputStream mirrorIn = null;
            if (targets.length > 0) {
                DatanodeInfo mirror = targets[0];
                String firstBadLink = mirror.getXferAddr();  // who is to blame if it goes wrong
                try {
                    mirrorSock = DataTransferProtocol.connect(mirror);
                    mirrorOut = new DataOutputStream(new BufferedOutputStream(mirrorSock.getOutputStream(), 128 * 1024));
                    mirrorIn = new DataInputStream(new BufferedInputStream(mirrorSock.getInputStream()));
                    DataTransferProtocol.sendWriteBlock(mirrorOut, block, clientName,
                            Arrays.copyOfRange(targets, 1, targets.length));
                    short status = mirrorIn.readShort();
                    String downstreamBadLink = mirrorIn.readUTF();
                    if (status != DataTransferProtocol.SUCCESS) {
                        if (!downstreamBadLink.isEmpty()) {
                            firstBadLink = downstreamBadLink;
                        }
                        throw new IOException("Pipeline setup failed downstream at " + firstBadLink);
                    }
                } catch (IOException e) {
                    DataTransferProtocol.sendResponse(out, DataTransferProtocol.ERROR, firstBadLink);
                    throw e;
                }
            }
            // setup ack upstream: the pipeline is established
            DataTransferProtocol.sendResponse(out, DataTransferProtocol.SUCCESS, "");
            new BlockReceiver(datanode, replica, socket, in, out, mirrorOut, mirrorIn).receiveBlock();
        } finally {
            replica.clearWriter(Thread.currentThread());
            if (mirrorSock != null) {
                mirrorSock.close();
            }
        }
    }

    /** OP_READ_BLOCK: send the status, then the packets. */
    private void readBlock(DataInputStream in, DataOutputStream out) throws IOException {
        Block block = DataTransferProtocol.readBlock(in);
        long offset = in.readLong();
        long length = in.readLong();
        BlockSender sender;
        try {
            sender = new BlockSender(datanode.getFSDataset(), block, offset, length);
        } catch (IOException e) {
            DataTransferProtocol.sendResponse(out, DataTransferProtocol.ERROR, e.getMessage());
            throw e;
        }
        try (BlockSender s = sender) {
            DataTransferProtocol.sendResponse(out, DataTransferProtocol.SUCCESS, "");
            s.sendBlock(out);
        }
    }
}
