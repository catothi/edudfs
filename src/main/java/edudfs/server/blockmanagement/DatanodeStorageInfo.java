package edudfs.server.blockmanagement;

import edudfs.protocol.StorageType;
import edudfs.server.protocol.DatanodeStorage;
import edudfs.server.protocol.StorageReport;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo:
 * the NameNode's view of ONE storage (volume/disk) of a DataNode.
 *
 * Since HDFS-2832 ("Heterogeneous Storage", Hadoop 2.4), the NameNode knows not only
 * DataNodes but also their individual storages: a replica lives on a storage, not
 * merely on a DataNode. This allows the NameNode to
 * <ul>
 *   <li>detect the failure of a single disk (storage -&gt; FAILED) and re-replicate exactly
 *       its replicas, without writing off the whole DataNode,</li>
 *   <li>take storage types (DISK, SSD, ...) into account during placement.</li>
 * </ul>
 * Simplification: the blocks are kept in a HashSet. In the original they form a doubly
 * linked list through the "triplets" of the BlockInfos (no extra memory per entry).
 */
public class DatanodeStorageInfo {

    /** Result of addBlock (as in the original). */
    public enum AddBlockResult {
        ADDED,
        REPLACED,
        ALREADY_EXIST
    }

    private final DatanodeDescriptor dn;
    private final String storageID;
    private StorageType storageType;
    private DatanodeStorage.State state;

    private long capacity;
    private long dfsUsed;
    private long remaining;
    /** How many full block reports have been processed for this storage. */
    private int blockReportCount;

    private final Set<BlockInfo> blocks = new HashSet<>();

    DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
        this.dn = dn;
        this.storageID = s.getStorageID();
        this.storageType = s.getStorageType();
        this.state = s.getState();
    }

    public DatanodeDescriptor getDatanodeDescriptor() {
        return dn;
    }

    public String getStorageID() {
        return storageID;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public DatanodeStorage.State getState() {
        return state;
    }

    void setState(DatanodeStorage.State state) {
        this.state = state;
    }

    void updateFromStorage(DatanodeStorage s) {
        this.storageType = s.getStorageType();
        this.state = s.getState();
    }

    void updateState(StorageReport r) {
        this.capacity = r.getCapacity();
        this.dfsUsed = r.getDfsUsed();
        this.remaining = r.getRemaining();
        if (r.isFailed()) {
            state = DatanodeStorage.State.FAILED;
        }
    }

    public long getCapacity() {
        return capacity;
    }

    public long getDfsUsed() {
        return dfsUsed;
    }

    public long getRemaining() {
        return remaining;
    }

    int getBlockReportCount() {
        return blockReportCount;
    }

    void receivedBlockReport() {
        blockReportCount++;
    }

    // ------------------------------------------------------------- Blocks
    /**
     * Records the block on this storage. If it was previously on a DIFFERENT storage
     * of the same DataNode (e.g. it ended up on another disk after a restart), it is
     * removed there - only one replica ever counts per DataNode.
     */
    AddBlockResult addBlock(BlockInfo b) {
        AddBlockResult result = AddBlockResult.ADDED;
        DatanodeStorageInfo other = b.findStorageInfo(dn);
        if (other != null) {
            if (other == this) {
                return AddBlockResult.ALREADY_EXIST;
            }
            other.removeBlock(b);
            result = AddBlockResult.REPLACED;
        }
        b.addStorage(this);
        blocks.add(b);
        return result;
    }

    boolean removeBlock(BlockInfo b) {
        blocks.remove(b);
        return b.removeStorage(this);
    }

    public Set<BlockInfo> getBlocks() {
        return Collections.unmodifiableSet(blocks);
    }

    public int numBlocks() {
        return blocks.size();
    }

    @Override
    public String toString() {
        return "[" + storageType + "]" + storageID + ":" + state + ":" + dn.getXferAddr();
    }
}
