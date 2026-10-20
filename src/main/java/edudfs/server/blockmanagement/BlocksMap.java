package edudfs.server.blockmanagement;

import edudfs.protocol.Block;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.BlocksMap.
 *
 * The second central in-memory structure of the NameNode (besides the INode tree):
 * Block -> BlockInfo (file + storages of the DataNodes).
 *
 * In the original this is a LightWeightGSet: a fixed-size hash table (2% of the heap)
 * whose entries (BlockInfo) carry the "next" reference themselves - no Map.Entry objects.
 * Since Block.equals() compares only the blockId, any Block with the same ID
 * suffices for the lookup.
 */
public class BlocksMap {

    private final Map<Block, BlockInfo> blocks = new HashMap<>();

    public BlockInfo getStoredBlock(Block b) {
        return blocks.get(b);
    }

    /** Inserts the block and links it to the file (INode ID). */
    BlockInfo addBlockCollection(BlockInfo b, long bcId) {
        BlockInfo info = blocks.get(b);
        if (info != b) {
            info = b;
            blocks.put(info, info);
        }
        info.setBlockCollectionId(bcId);
        return info;
    }

    /** Removes the block completely - also from the block lists of the storages. */
    void removeBlock(BlockInfo block) {
        BlockInfo info = blocks.remove(block);
        if (info != null) {
            for (DatanodeStorageInfo s : new ArrayList<>(info.getStorageInfos())) {
                s.removeBlock(info);
            }
            info.clearStorages();
            info.setBlockCollectionId(BlockInfo.INVALID_INODE_ID);
        }
    }

    public int size() {
        return blocks.size();
    }

    public Collection<BlockInfo> getBlocks() {
        return Collections.unmodifiableCollection(blocks.values());
    }
}
