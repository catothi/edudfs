package edudfs.server.protocol;

import edudfs.protocol.Block;
import edudfs.protocol.DatanodeInfo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.
 * With it, the NameNode instructs a "primary" DataNode to bring the replicas
 * of an incomplete block to a common length.
 */
public class BlockRecoveryCommand extends DatanodeCommand {

    private static final long serialVersionUID = 1L;

    private final List<RecoveringBlock> recoveringBlocks;

    public BlockRecoveryCommand(List<RecoveringBlock> recoveringBlocks) {
        super(DatanodeProtocol.DNA_RECOVERBLOCK);
        this.recoveringBlocks = recoveringBlocks;
    }

    public List<RecoveringBlock> getRecoveringBlocks() {
        return recoveringBlocks;
    }

    /** A block that is to be recovered. */
    public static class RecoveringBlock implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Block block;               // with the OLD generation stamp
        private final DatanodeInfo[] locations;  // expected replica locations (pipeline)
        private final long newGenerationStamp;   // = recoveryId

        public RecoveringBlock(Block block, DatanodeInfo[] locations, long newGenerationStamp) {
            this.block = block;
            this.locations = locations;
            this.newGenerationStamp = newGenerationStamp;
        }

        public Block getBlock() {
            return block;
        }

        public DatanodeInfo[] getLocations() {
            return locations;
        }

        public long getNewGenerationStamp() {
            return newGenerationStamp;
        }

        @Override
        public String toString() {
            return "RecoveringBlock{" + block + ", newGS=" + newGenerationStamp + ", locs="
                    + Arrays.toString(locations) + "}";
        }
    }
}
