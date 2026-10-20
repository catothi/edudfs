package edudfs.server.protocol;

import edudfs.protocol.Block;
import edudfs.protocol.DatanodeID;

import java.io.IOException;
import java.rmi.Remote;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol.
 *
 * Important: the NameNode NEVER actively calls a DataNode. All commands
 * (replicate, delete, block recovery) are returned as the response to a
 * heartbeat.
 */
public interface DatanodeProtocol extends Remote {

    String NAME = "DatanodeProtocol";

    // Command codes - identical to the original
    int DNA_UNKNOWN = 0;
    int DNA_TRANSFER = 1;      // copy block to other DataNodes (re-replication)
    int DNA_INVALIDATE = 2;    // delete blocks
    int DNA_SHUTDOWN = 3;
    int DNA_REGISTER = 4;      // "I don't know you - register again"
    int DNA_FINALIZE = 5;
    int DNA_RECOVERBLOCK = 6;  // perform block recovery (lease recovery) as primary

    DatanodeID registerDatanode(DatanodeID registration) throws IOException;

    /**
     * Heartbeat: one StorageReport per healthy volume plus the number of failed volumes.
     * If failedVolumes increases, all storages that are no longer reported are considered FAILED.
     */
    DatanodeCommand[] sendHeartbeat(DatanodeID registration, StorageReport[] reports, int failedVolumes)
            throws IOException;

    /** Full block report: all replicas of this DataNode, grouped by storage. */
    DatanodeCommand blockReport(DatanodeID registration, String poolId, StorageBlockReport[] reports)
            throws IOException;

    /** Incremental block report (IBR): newly received or deleted blocks, grouped by storage. */
    void blockReceivedAndDeleted(DatanodeID registration, String poolId, StorageReceivedDeletedBlocks[] blocks)
            throws IOException;

    /**
     * Completion of a block recovery by the primary DataNode. newTargetStorages[i] is the
     * storageID of the replica on newTargets[i].
     */
    void commitBlockSynchronization(Block block, long newGenerationStamp, long newLength, boolean closeFile,
                                    boolean deleteBlock, DatanodeID[] newTargets, String[] newTargetStorages)
            throws IOException;
}
