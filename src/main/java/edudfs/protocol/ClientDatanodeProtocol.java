package edudfs.protocol;

import java.io.IOException;
import java.rmi.Remote;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol: RPC from the client
 * directly to a DataNode (RMI, ipcPort).
 *
 * Needed to read a file that is still being written: the NameNode only knows the length of the
 * completed blocks. How many bytes the last block (RBW replica) currently has, only the DataNodes
 * of the pipeline know.
 */
public interface ClientDatanodeProtocol extends Remote {

    String NAME = "ClientDatanodeProtocol";

    /** Current length of the replica (for an RBW replica: everything received so far). */
    long getReplicaVisibleLength(Block b) throws IOException;
}
