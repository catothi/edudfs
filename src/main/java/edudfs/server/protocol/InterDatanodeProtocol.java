package edudfs.server.protocol;

import edudfs.protocol.Block;

import java.io.IOException;
import java.rmi.Remote;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol.
 * Used exclusively for block recovery: the primary DataNode asks all
 * participating DataNodes for their replica and aligns them.
 */
public interface InterDatanodeProtocol extends Remote {

    String NAME = "InterDatanodeProtocol";

    /**
     * Stops a writer that may still be running, sets the replica to RUR
     * and returns length/GS/original state. null = no replica present.
     */
    ReplicaRecoveryInfo initReplicaRecovery(BlockRecoveryCommand.RecoveringBlock rBlock) throws IOException;

    /**
     * Truncates the replica to newLength, sets GS = recoveryId and finalizes it.
     * Returns: storageID of the volume holding the replica.
     */
    String updateReplicaUnderRecovery(Block oldBlock, long recoveryId, long newLength) throws IOException;
}
