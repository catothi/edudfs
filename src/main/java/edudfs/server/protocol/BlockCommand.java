package edudfs.server.protocol;

import edudfs.protocol.Block;
import edudfs.protocol.DatanodeInfo;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.BlockCommand.
 * DNA_TRANSFER: copy blocks[i] to targets[i]. DNA_INVALIDATE: delete blocks.
 */
public class BlockCommand extends DatanodeCommand {

    private static final long serialVersionUID = 1L;

    private final Block[] blocks;
    private final DatanodeInfo[][] targets;

    public BlockCommand(int action, Block[] blocks, DatanodeInfo[][] targets) {
        super(action);
        this.blocks = blocks;
        this.targets = targets;
    }

    public Block[] getBlocks() {
        return blocks;
    }

    public DatanodeInfo[][] getTargets() {
        return targets;
    }
}
