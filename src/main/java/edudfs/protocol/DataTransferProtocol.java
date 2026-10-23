package edudfs.protocol;

import edudfs.common.HdfsConfig;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol (+ Sender/Receiver).
 *
 * Bulk data does NOT go over RPC but over a separate TCP connection
 * to the DataNode's xferPort (in the original: a streaming protocol with Protobuf headers).
 *
 * <pre>
 * Request:  short version | byte op | op-specific header
 * Response: short status  | UTF message (for WRITE_BLOCK: firstBadLink)
 * then:     packets (see {@link Packet}) or acks (see {@link PipelineAck})
 * </pre>
 */
public final class DataTransferProtocol {

    public static final short DATA_TRANSFER_VERSION = 28;

    public static final byte OP_WRITE_BLOCK = 80;
    public static final byte OP_READ_BLOCK = 81;

    public static final short SUCCESS = 0;
    public static final short ERROR = 1;

    public static final int CONNECT_TIMEOUT_MS = 5000;
    public static final int READ_TIMEOUT_MS = (int) HdfsConfig.SOCKET_TIMEOUT_MS;

    private DataTransferProtocol() {
    }

    // --------------------------------------------------------- Serialization
    public static void writeBlock(DataOutputStream out, Block b) throws IOException {
        out.writeLong(b.getBlockId());
        out.writeLong(b.getNumBytes());
        out.writeLong(b.getGenerationStamp());
    }

    public static Block readBlock(DataInputStream in) throws IOException {
        return new Block(in.readLong(), in.readLong(), in.readLong());
    }

    public static void writeDatanodeInfo(DataOutputStream out, DatanodeID dn) throws IOException {
        out.writeUTF(dn.getDatanodeUuid());
        out.writeUTF(dn.getHostName());
        out.writeInt(dn.getXferPort());
        out.writeInt(dn.getIpcPort());
    }

    public static DatanodeInfo readDatanodeInfo(DataInputStream in) throws IOException {
        return new DatanodeInfo(new DatanodeID(in.readUTF(), in.readUTF(), in.readInt(), in.readInt()));
    }

    // ------------------------------------------------------------ Requests
    /**
     * OP_WRITE_BLOCK: sets up a pipeline. 'targets' are the DataNodes AFTER the receiver -
     * each DataNode removes itself and passes the rest on to its successor ("mirror").
     */
    public static void sendWriteBlock(DataOutputStream out, Block b, String clientName, DatanodeInfo[] targets)
            throws IOException {
        out.writeShort(DATA_TRANSFER_VERSION);
        out.writeByte(OP_WRITE_BLOCK);
        writeBlock(out, b);
        out.writeUTF(clientName);
        out.writeInt(targets.length);
        for (DatanodeInfo t : targets) {
            writeDatanodeInfo(out, t);
        }
        out.flush();
    }

    public static void sendReadBlock(DataOutputStream out, Block b, long offset, long length) throws IOException {
        out.writeShort(DATA_TRANSFER_VERSION);
        out.writeByte(OP_READ_BLOCK);
        writeBlock(out, b);
        out.writeLong(offset);
        out.writeLong(length);
        out.flush();
    }

    public static void sendResponse(DataOutputStream out, short status, String message) throws IOException {
        out.writeShort(status);
        out.writeUTF(message == null ? "" : message);
        out.flush();
    }

    public static Socket connect(DatanodeID dn) throws IOException {
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(dn.getHostName(), dn.getXferPort()), CONNECT_TIMEOUT_MS);
            s.setSoTimeout(READ_TIMEOUT_MS);
            s.setTcpNoDelay(true);
            return s;
        } catch (IOException e) {
            s.close();
            throw e;
        }
    }
}
