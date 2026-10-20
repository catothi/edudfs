package edudfs.server.protocol;

import edudfs.protocol.Block;
import edudfs.server.common.HdfsServerConstants.ReplicaState;

/** Corresponds to org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo. */
public class ReplicaRecoveryInfo extends Block {

    private static final long serialVersionUID = 1L;

    private final ReplicaState originalState;

    public ReplicaRecoveryInfo(long blockId, long diskLen, long gs, ReplicaState originalState) {
        super(blockId, diskLen, gs);
        this.originalState = originalState;
    }

    public ReplicaState getOriginalReplicaState() {
        return originalState;
    }
}
