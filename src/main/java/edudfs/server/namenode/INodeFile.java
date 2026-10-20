package edudfs.server.namenode;

import edudfs.server.blockmanagement.BlockInfo;

import java.util.Arrays;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.INodeFile.
 *
 * In the NameNode a file is just: metadata + an array of BlockInfos.
 * The actual data lives exclusively on the DataNodes.
 */
public class INodeFile extends INode {

    /**
     * As in the original, replication factor and block size are packed into ONE long
     * to save memory per file (original: 4 bits storage policy | 12 bits
     * layout+replication | 48 bits preferredBlockSize).
     */
    static final class HeaderFormat {
        private static final int BLOCK_SIZE_BITS = 48;
        private static final long BLOCK_SIZE_MASK = (1L << BLOCK_SIZE_BITS) - 1;
        private static final long REPLICATION_MASK = (1L << 11) - 1;

        static long toLong(long preferredBlockSize, short replication) {
            return ((replication & REPLICATION_MASK) << BLOCK_SIZE_BITS) | (preferredBlockSize & BLOCK_SIZE_MASK);
        }

        static short getReplication(long header) {
            return (short) ((header >>> BLOCK_SIZE_BITS) & REPLICATION_MASK);
        }

        static long getPreferredBlockSize(long header) {
            return header & BLOCK_SIZE_MASK;
        }
    }

    private long header;
    private BlockInfo[] blocks = BlockInfo.EMPTY_ARRAY;
    private FileUnderConstructionFeature ucFeature;

    public INodeFile(long id, String name, long mtime, short replication, long preferredBlockSize) {
        super(id, name, mtime);
        this.header = HeaderFormat.toLong(preferredBlockSize, replication);
    }

    @Override
    public boolean isFile() {
        return true;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public INodeFile asFile() {
        return this;
    }

    public short getFileReplication() {
        return HeaderFormat.getReplication(header);
    }

    public long getPreferredBlockSize() {
        return HeaderFormat.getPreferredBlockSize(header);
    }

    // ------------------------------------------------------------- Blocks
    public BlockInfo[] getBlocks() {
        return blocks;
    }

    public int numBlocks() {
        return blocks.length;
    }

    public BlockInfo getLastBlock() {
        return blocks.length == 0 ? null : blocks[blocks.length - 1];
    }

    public BlockInfo getPenultimateBlock() {
        return blocks.length < 2 ? null : blocks[blocks.length - 2];
    }

    void addBlock(BlockInfo b) {
        blocks = Arrays.copyOf(blocks, blocks.length + 1);
        blocks[blocks.length - 1] = b;
    }

    boolean removeLastBlock(BlockInfo b) {
        if (blocks.length == 0 || !blocks[blocks.length - 1].equals(b)) {
            return false;
        }
        blocks = Arrays.copyOf(blocks, blocks.length - 1);
        return true;
    }

    /**
     * File size = sum of the block lengths. The length of the last block of an open file
     * is usually not yet known to the NameNode (it only arrives with commit).
     */
    public long computeFileSize(boolean includesLastUcBlock) {
        if (blocks.length == 0) {
            return 0;
        }
        long size = 0;
        for (int i = 0; i < blocks.length - 1; i++) {
            size += blocks[i].getNumBytes();
        }
        BlockInfo last = blocks[blocks.length - 1];
        if (last.isComplete() || includesLastUcBlock) {
            size += last.getNumBytes();
        }
        return size;
    }

    // ------------------------------------------------- Under Construction
    public boolean isUnderConstruction() {
        return ucFeature != null;
    }

    public FileUnderConstructionFeature getFileUnderConstructionFeature() {
        return ucFeature;
    }

    void toUnderConstruction(String clientName, String clientMachine) {
        ucFeature = new FileUnderConstructionFeature(clientName, clientMachine);
    }

    void toCompleteFile(long mtime) {
        for (BlockInfo b : blocks) {
            if (!b.isComplete()) {
                throw new IllegalStateException("Cannot close " + getFullPathName() + ": block " + b + " is "
                        + b.getBlockUCState());
            }
        }
        ucFeature = null;
        setModificationTime(mtime);
    }

    @Override
    void destroyAndCollectBlocks(List<BlockInfo> collectedBlocks, List<INode> removedINodes) {
        collectedBlocks.addAll(Arrays.asList(blocks));
        blocks = BlockInfo.EMPTY_ARRAY;
        removedINodes.add(this);
    }
}
