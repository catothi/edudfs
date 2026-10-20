package edudfs.protocol;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.LocatedBlock:
 * a block, its offset in the file and the DataNodes that hold it
 * (or, when writing: the pipeline targets).
 */
public class LocatedBlock implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Block block;
    private final long startOffset;
    private final DatanodeInfo[] locations;

    public LocatedBlock(Block block, long startOffset, DatanodeInfo[] locations) {
        this.block = block;
        this.startOffset = startOffset;
        this.locations = locations;
    }

    public Block getBlock() {
        return block;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public DatanodeInfo[] getLocations() {
        return locations;
    }

    @Override
    public String toString() {
        return "LocatedBlock{" + block + ", len=" + block.getNumBytes() + ", offset=" + startOffset
                + ", locs=" + Arrays.toString(locations) + "}";
    }
}
