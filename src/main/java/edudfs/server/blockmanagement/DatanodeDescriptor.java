package edudfs.server.blockmanagement;

import edudfs.protocol.Block;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.StorageType;
import edudfs.server.protocol.DatanodeStorage;
import edudfs.server.protocol.StorageReport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor:
 * the NameNode-internal view of a DataNode.
 *
 * Contains (a) the storages (disks) of the DataNode - each with its blocks - and
 * (b) queues of work that is delivered as a command with the next heartbeat.
 */
public class DatanodeDescriptor extends DatanodeInfo {

    private static final long serialVersionUID = 1L;

    /** Block + targets for a re-replication command. */
    public static class BlockTargetPair {
        public final Block block;
        public final DatanodeStorageInfo[] targets;

        BlockTargetPair(Block block, DatanodeStorageInfo[] targets) {
            this.block = block;
            this.targets = targets;
        }
    }

    /** storageID -> Storage (Original: storageMap). */
    private final transient Map<String, DatanodeStorageInfo> storageMap = new LinkedHashMap<>();
    /** Number of failed volumes according to the last heartbeat. */
    private int volumeFailures = 0;
    private final transient Queue<BlockTargetPair> replicateBlocks = new ArrayDeque<>();
    /** In the original: InvalidateBlocks (separate class in the BlockManager, per DataNode). */
    private final transient Set<Block> invalidateBlocks = new LinkedHashSet<>();
    private final transient Set<BlockInfo> recoverBlocks = new LinkedHashSet<>();

    public DatanodeDescriptor(DatanodeID id) {
        super(id);
    }

    // ----------------------------------------------------------- Storages
    public DatanodeStorageInfo getStorageInfo(String storageID) {
        return storageMap.get(storageID);
    }

    public Collection<DatanodeStorageInfo> getStorageInfos() {
        return Collections.unmodifiableCollection(storageMap.values());
    }

    /** Create the storage or update its type/state (from heartbeat, block report or IBR). */
    DatanodeStorageInfo updateStorage(DatanodeStorage s) {
        DatanodeStorageInfo storage = storageMap.get(s.getStorageID());
        if (storage == null) {
            storage = new DatanodeStorageInfo(this, s);
            storageMap.put(s.getStorageID(), storage);
        } else if (storage.getState() != s.getState() || storage.getStorageType() != s.getStorageType()) {
            storage.updateFromStorage(s);
        }
        return storage;
    }

    void removeStorage(DatanodeStorageInfo storage) {
        storageMap.remove(storage.getStorageID());
    }

    /**
     * Like DatanodeDescriptor.updateHeartbeatState: take over the capacities. If the number of
     * failed volumes has increased, all storages for which NO report arrives anymore are
     * considered failed. Returns the storages newly set to FAILED in the process.
     */
    List<DatanodeStorageInfo> updateHeartbeatState(StorageReport[] reports, int volFailures) {
        boolean checkFailedStorages = volFailures > this.volumeFailures;
        Set<DatanodeStorageInfo> failedStorageInfos = checkFailedStorages ? new HashSet<>(storageMap.values()) : null;
        this.volumeFailures = volFailures;
        long used = 0;
        for (StorageReport r : reports) {
            DatanodeStorageInfo storage = updateStorage(r.getStorage());
            if (checkFailedStorages) {
                failedStorageInfos.remove(storage);
            }
            storage.updateState(r);
            used += r.getDfsUsed();
        }
        setDfsUsed(used);
        List<DatanodeStorageInfo> newlyFailed = new ArrayList<>();
        if (failedStorageInfos != null) {
            for (DatanodeStorageInfo storage : failedStorageInfos) {
                if (storage.getState() != DatanodeStorage.State.FAILED) {
                    storage.setState(DatanodeStorage.State.FAILED);
                    newlyFailed.add(storage);
                }
            }
        }
        return newlyFailed;
    }

    public int getVolumeFailures() {
        return volumeFailures;
    }

    /**
     * Like DatanodeDescriptor.chooseStorage4Block: a healthy storage of the desired type
     * with enough space for a whole block (the one with the most free space).
     */
    DatanodeStorageInfo chooseStorage4Block(StorageType t, long blockSize) {
        DatanodeStorageInfo best = null;
        for (DatanodeStorageInfo s : storageMap.values()) {
            if (s.getState() == DatanodeStorage.State.NORMAL && s.getStorageType() == t
                    && s.getRemaining() >= blockSize && (best == null || s.getRemaining() > best.getRemaining())) {
                best = s;
            }
        }
        return best;
    }

    /** Free space across all healthy storages. */
    @Override
    public long getRemaining() {
        long sum = 0;
        for (DatanodeStorageInfo s : storageMap.values()) {
            if (s.getState() == DatanodeStorage.State.NORMAL) {
                sum += s.getRemaining();
            }
        }
        return sum;
    }

    @Override
    public long getCapacity() {
        long sum = 0;
        for (DatanodeStorageInfo s : storageMap.values()) {
            if (s.getState() == DatanodeStorage.State.NORMAL) {
                sum += s.getCapacity();
            }
        }
        return sum;
    }

    // ------------------------------------------------------------- Blocks
    /** All blocks across all storages. */
    public Set<BlockInfo> getBlocks() {
        Set<BlockInfo> r = new HashSet<>();
        for (DatanodeStorageInfo s : storageMap.values()) {
            r.addAll(s.getBlocks());
        }
        return r;
    }

    @Override
    public int getNumBlocks() {
        int n = 0;
        for (DatanodeStorageInfo s : storageMap.values()) {
            n += s.numBlocks();
        }
        return n;
    }

    // ------------------------------------------------------- Work queues
    void addBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets) {
        replicateBlocks.add(new BlockTargetPair(block, targets));
    }

    List<BlockTargetPair> getReplicationCommand(int max) {
        List<BlockTargetPair> result = new ArrayList<>();
        while (result.size() < max && !replicateBlocks.isEmpty()) {
            result.add(replicateBlocks.poll());
        }
        return result;
    }

    public int getNumberOfBlocksToBeReplicated() {
        return replicateBlocks.size();
    }

    void addBlockToBeInvalidated(Block b) {
        invalidateBlocks.add(b);
    }

    boolean containsInvalidateBlock(Block b) {
        return invalidateBlocks.contains(b);
    }

    public Set<Block> getPendingInvalidations() {
        return Collections.unmodifiableSet(invalidateBlocks);
    }

    List<Block> getInvalidateBlocks(int max) {
        List<Block> result = new ArrayList<>();
        Iterator<Block> it = invalidateBlocks.iterator();
        while (result.size() < max && it.hasNext()) {
            result.add(it.next());
            it.remove();
        }
        return result;
    }

    void addBlockToBeRecovered(BlockInfo b) {
        recoverBlocks.add(b);
    }

    List<BlockInfo> getLeaseRecoveryCommand(int max) {
        List<BlockInfo> result = new ArrayList<>();
        Iterator<BlockInfo> it = recoverBlocks.iterator();
        while (result.size() < max && it.hasNext()) {
            result.add(it.next());
            it.remove();
        }
        return result;
    }

    /** On death / re-registration: forget commands (blocks have already been removed). */
    void resetBlocksAndQueues() {
        replicateBlocks.clear();
        invalidateBlocks.clear();
        recoverBlocks.clear();
    }
}
