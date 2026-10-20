package edudfs.server.blockmanagement;

import edudfs.common.SequentialNumber;
import edudfs.protocol.Block;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.SequentialBlockIdGenerator.
 *
 * Since HDFS-4645 (Hadoop 2.1), block IDs are allocated sequentially, starting after
 * LAST_RESERVED_BLOCK_ID = 2^30. The first block of a fresh cluster is therefore named
 * blk_1073741825. (Older clusters had random IDs - which is why allocation checks
 * whether the ID is already taken.)
 */
public class SequentialBlockIdGenerator extends SequentialNumber {

    public static final long LAST_RESERVED_BLOCK_ID = 1024L * 1024 * 1024;

    private final BlockManager blockManager;

    SequentialBlockIdGenerator(BlockManager blockManager) {
        super(LAST_RESERVED_BLOCK_ID);
        this.blockManager = blockManager;
    }

    @Override
    public long nextValue() {
        Block b = new Block(super.nextValue(), 0, 0);
        // Skip IDs already in use (collision with "legacy" random IDs)
        while (blockManager.getStoredBlock(b) != null) {
            b.setBlockId(super.nextValue());
        }
        return b.getBlockId();
    }
}
