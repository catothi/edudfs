package edudfs.protocol;

import java.io.IOException;
import java.rmi.Remote;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.ClientProtocol:
 * the RPC interface between the client (DFSClient) and the NameNode.
 *
 * In the original this runs over Hadoop RPC + Protobuf; here over Java RMI.
 * All methods throw IOException (RemoteException is a subclass).
 */
public interface ClientProtocol extends Remote {

    String NAME = "ClientProtocol";

    // ------------------------------------------------------------- Reading
    LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException;

    // ------------------------------------------------------------- Writing
    /** Creates an empty file "under construction" and grants a lease to clientName. */
    HdfsFileStatus create(String src, String clientName, boolean overwrite, short replication, long blockSize)
            throws IOException;

    /**
     * Reopens a closed file for writing and grants a lease to clientName.
     * Corresponds to append(src, clientName, flag = {APPEND, NEW_BLOCK}): the new data
     * always starts in a new block; the current last block stays unchanged.
     */
    LastBlockWithStatus append(String src, String clientName) throws IOException;

    /**
     * Commits the previous block (with its final length) and allocates a new
     * block including pipeline targets.
     */
    LocatedBlock addBlock(String src, String clientName, Block previous, long fileId, DatanodeInfo[] excludeNodes)
            throws IOException;

    /** Abandons a just-allocated block (e.g. when the pipeline setup fails). */
    void abandonBlock(Block b, long fileId, String src, String holder) throws IOException;

    /**
     * Closes the file. Returns false as long as the last block is not yet
     * minimally replicated - the client then retries.
     */
    boolean complete(String src, String clientName, Block last, long fileId) throws IOException;

    /** The client reports a corrupt replica (checksum error while reading). */
    void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

    // -------------------------------------------------------------- Leases
    /** Renews all of the client's leases ("I am still alive"). */
    void renewLease(String clientName) throws IOException;

    /** Forces lease recovery (like 'hdfs debug recoverLease'). true = the file is closed. */
    boolean recoverLease(String src, String clientName) throws IOException;

    // ----------------------------------------------------------- Namespace
    boolean mkdirs(String src) throws IOException;

    boolean delete(String src, boolean recursive) throws IOException;

    boolean rename(String src, String dst) throws IOException;

    HdfsFileStatus getFileInfo(String src) throws IOException;

    HdfsFileStatus[] getListing(String src) throws IOException;

    // ------------------------------------------------------- Admin/teaching
    DatanodeInfo[] getDatanodeReport() throws IOException;

    /** Dump of the in-memory structures (like 'hdfs dfsadmin -metasave', but more detailed). */
    String metaSave() throws IOException;
}
