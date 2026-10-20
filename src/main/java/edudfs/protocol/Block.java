package edudfs.protocol;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.Block.
 *
 * A block is described by three longs:
 * <ul>
 *   <li>blockId         - unique ID (assigned sequentially starting at 2^30 + 1)</li>
 *   <li>numBytes        - length in bytes</li>
 *   <li>generationStamp - "version" of the block; incremented on every recovery
 *       so that stale replicas can be detected</li>
 * </ul>
 * Important: equals/hashCode consider ONLY the blockId (as in the original).
 * This lets a block with any GS/length serve as a key in the BlocksMap.
 */
public class Block implements Serializable, Comparable<Block> {

    private static final long serialVersionUID = 1L;

    public static final String BLOCK_FILE_PREFIX = "blk_";
    public static final String METADATA_EXTENSION = ".meta";

    private long blockId;
    private long numBytes;
    private long generationStamp;

    public Block(long blockId, long numBytes, long generationStamp) {
        this.blockId = blockId;
        this.numBytes = numBytes;
        this.generationStamp = generationStamp;
    }

    public Block(Block b) {
        this(b.blockId, b.numBytes, b.generationStamp);
    }

    public long getBlockId() {
        return blockId;
    }

    public void setBlockId(long blockId) {
        this.blockId = blockId;
    }

    public long getNumBytes() {
        return numBytes;
    }

    public void setNumBytes(long numBytes) {
        this.numBytes = numBytes;
    }

    public long getGenerationStamp() {
        return generationStamp;
    }

    public void setGenerationStamp(long generationStamp) {
        this.generationStamp = generationStamp;
    }

    /** File name of the block on the DataNode, e.g. "blk_1073741825". */
    public String getBlockName() {
        return BLOCK_FILE_PREFIX + blockId;
    }

    @Override
    public String toString() {
        return getBlockName() + "_" + generationStamp;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Block && ((Block) o).blockId == blockId);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(blockId);
    }

    @Override
    public int compareTo(Block b) {
        return Long.compare(blockId, b.blockId);
    }
}
