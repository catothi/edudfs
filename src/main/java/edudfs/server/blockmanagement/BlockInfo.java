package edudfs.server.blockmanagement;

import edudfs.protocol.Block;
import edudfs.server.common.HdfsServerConstants.BlockUCState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo(Contiguous).
 *
 * This is the entry in the BlocksMap. It links a block to
 * <ul>
 *   <li>the file (bcId = INode ID of the "BlockCollection") and</li>
 *   <li>the storages (disks of DataNodes) that have reported a FINALIZED replica -
 *       at most one per DataNode.</li>
 * </ul>
 * Simplification: in the original, the storages are kept in an Object[] "triplets"
 * (storage, prev, next), which additionally forms a doubly linked list of all blocks
 * per storage - without extra memory for list nodes.
 * Here: a simple ArrayList + a HashSet in DatanodeStorageInfo.
 */
public class BlockInfo extends Block {

    private static final long serialVersionUID = 1L;

    public static final BlockInfo[] EMPTY_ARRAY = {};

    /** bcId for blocks whose file has been deleted. */
    public static final long INVALID_INODE_ID = -1;

    private short replication;
    private long bcId = INVALID_INODE_ID;
    private final transient List<DatanodeStorageInfo> storages = new ArrayList<>(3);

    /** Only set while the block is not COMPLETE. */
    private transient BlockUnderConstructionFeature uc;

    public BlockInfo(Block blk, short replication) {
        super(blk);
        this.replication = replication;
    }

    public short getReplication() {
        return replication;
    }

    public void setReplication(short replication) {
        this.replication = replication;
    }

    public long getBlockCollectionId() {
        return bcId;
    }

    public void setBlockCollectionId(long id) {
        this.bcId = id;
    }

    public boolean isDeleted() {
        return bcId == INVALID_INODE_ID;
    }

    // ------------------------------------------------------------ Storages
    /** Call only via DatanodeStorageInfo.addBlock (keeps both sides consistent). */
    boolean addStorage(DatanodeStorageInfo storage) {
        if (storages.contains(storage)) {
            return false;
        }
        storages.add(storage);
        return true;
    }

    boolean removeStorage(DatanodeStorageInfo storage) {
        return storages.remove(storage);
    }

    void clearStorages() {
        storages.clear();
    }

    public List<DatanodeStorageInfo> getStorageInfos() {
        return Collections.unmodifiableList(storages);
    }

    /** The DataNodes holding a replica (original: getDatanode(i) for i &lt; numNodes()). */
    public List<DatanodeDescriptor> getDatanodes() {
        List<DatanodeDescriptor> r = new ArrayList<>(storages.size());
        for (DatanodeStorageInfo s : storages) {
            r.add(s.getDatanodeDescriptor());
        }
        return r;
    }

    /** The storage on which the DataNode holds this replica - or null. */
    public DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
        for (DatanodeStorageInfo s : storages) {
            if (s.getDatanodeDescriptor() == dn) {
                return s;
            }
        }
        return null;
    }

    public int numNodes() {
        return storages.size();
    }

    // ------------------------------------------------- Under Construction
    public BlockUCState getBlockUCState() {
        return uc == null ? BlockUCState.COMPLETE : uc.getBlockUCState();
    }

    public boolean isComplete() {
        return getBlockUCState() == BlockUCState.COMPLETE;
    }

    public boolean isCompleteOrCommitted() {
        BlockUCState s = getBlockUCState();
        return s == BlockUCState.COMPLETE || s == BlockUCState.COMMITTED;
    }

    public BlockUnderConstructionFeature getUnderConstructionFeature() {
        return uc;
    }

    public void convertToBlockUnderConstruction(BlockUCState s, DatanodeStorageInfo[] targets) {
        if (uc == null) {
            uc = new BlockUnderConstructionFeature(s, targets);
        } else {
            uc.setBlockUCState(s);
            uc.setExpectedLocations(targets);
        }
    }

    void convertToCompleteBlock() {
        uc = null;
    }
}
