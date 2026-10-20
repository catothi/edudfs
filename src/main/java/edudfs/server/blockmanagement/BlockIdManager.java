package edudfs.server.blockmanagement;

import edudfs.server.common.GenerationStamp;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager:
 * manages the block ID generator and the generation stamp.
 * (In the original, both counters are persisted in the EditLog/FSImage.)
 */
public class BlockIdManager {

    private final GenerationStamp generationStamp = new GenerationStamp();
    private final SequentialBlockIdGenerator blockIdGenerator;

    public BlockIdManager(BlockManager blockManager) {
        this.blockIdGenerator = new SequentialBlockIdGenerator(blockManager);
    }

    public long nextBlockId() {
        return blockIdGenerator.nextValue();
    }

    public long nextGenerationStamp() {
        return generationStamp.nextValue();
    }

    public long getGenerationStamp() {
        return generationStamp.getCurrentValue();
    }

    public long getLastAllocatedBlockId() {
        return blockIdGenerator.getCurrentValue();
    }
}
