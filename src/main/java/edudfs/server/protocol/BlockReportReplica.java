package edudfs.server.protocol;

import edudfs.protocol.Block;
import edudfs.server.common.HdfsServerConstants.ReplicaState;

import java.io.Serializable;

/**
 * An entry in the full block report
 * (in the original: BlockListAsLongs.BlockReportReplica - encoded compactly as long[] there).
 */
public class BlockReportReplica implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Block block;
    private final ReplicaState state;

    public BlockReportReplica(Block block, ReplicaState state) {
        this.block = block;
        this.state = state;
    }

    public Block getBlock() {
        return block;
    }

    public ReplicaState getState() {
        return state;
    }
}
