package edudfs.server.blockmanagement;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.common.Time;
import edudfs.protocol.Block;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.server.common.HdfsServerConstants.BlockUCState;
import edudfs.server.common.HdfsServerConstants.ReplicaState;
import edudfs.server.namenode.FSNamesystem;
import edudfs.server.namenode.INodeFile;
import edudfs.server.protocol.BlockReportReplica;
import edudfs.server.protocol.DatanodeStorage;
import edudfs.server.protocol.ReceivedDeletedBlockInfo;
import edudfs.server.protocol.StorageBlockReport;
import edudfs.server.protocol.StorageReceivedDeletedBlocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.BlockManager.
 *
 * Responsible for everything "below" the namespace:
 * <ul>
 *   <li>maintaining the BlocksMap (from block reports and IBRs of the DataNodes)</li>
 *   <li>committing/completing blocks</li>
 *   <li>detecting and fixing under-/over-replication (RedundancyMonitor)</li>
 *   <li>distributing deletion commands (invalidates)</li>
 * </ul>
 * All methods run under the FSNamesystem write lock (as in the original, "global lock").
 */
public class BlockManager {

    private static final String LOG = "BlockManager";

    private final FSNamesystem namesystem;
    private final BlocksMap blocksMap = new BlocksMap();
    private final DatanodeManager datanodeManager;
    private final BlockPlacementPolicy placementPolicy = new BlockPlacementPolicy();

    /** In the original: LowRedundancyBlocks with 5 priority queues. */
    private final Set<BlockInfo> neededReconstruction = new LinkedHashSet<>();
    /** In the original: PendingReconstructionBlocks. Block -> start time (monotonic). */
    private final Map<BlockInfo, Long> pendingReconstruction = new LinkedHashMap<>();

    private volatile boolean running;
    private Thread redundancyThread;

    public BlockManager(FSNamesystem namesystem) {
        this.namesystem = namesystem;
        this.datanodeManager = new DatanodeManager(namesystem, this);
    }

    public DatanodeManager getDatanodeManager() {
        return datanodeManager;
    }

    public BlockInfo getStoredBlock(Block b) {
        return blocksMap.getStoredBlock(b);
    }

    public int getTotalBlocks() {
        return blocksMap.size();
    }

    public BlockInfo addBlockCollection(BlockInfo block, long bcId) {
        return blocksMap.addBlockCollection(block, bcId);
    }

    public static DatanodeInfo[] toDatanodeInfos(Collection<? extends DatanodeInfo> nodes) {
        DatanodeInfo[] r = new DatanodeInfo[nodes.size()];
        int i = 0;
        for (DatanodeInfo dn : nodes) {
            r[i++] = new DatanodeInfo(dn);  // copy, so that no server structures get serialized
        }
        return r;
    }

    // ===================================================== Write path
    /** Pipeline targets: one storage per DataNode (original: DatanodeStorageInfo[] from chooseTarget4NewBlock). */
    public DatanodeStorageInfo[] chooseTarget4NewBlock(int replication, Set<? extends DatanodeID> excluded,
                                                       long blockSize) {
        return placementPolicy.chooseTarget(replication, datanodeManager.getDatanodes(), excluded, blockSize);
    }

    /**
     * In addBlock()/complete() the client reports the final length of the last block:
     * UNDER_CONSTRUCTION -> COMMITTED, and immediately COMPLETE if enough
     * DataNodes have already reported a FINALIZED replica.
     */
    public boolean commitOrCompleteLastBlock(INodeFile file, Block commitBlock) throws IOException {
        BlockInfo last = file.getLastBlock();
        if (last == null || commitBlock == null) {
            return false;
        }
        if (!last.equals(commitBlock)) {
            throw new IOException("Trying to commit " + commitBlock + " but last block is " + last);
        }
        if (last.isCompleteOrCommitted()) {
            return false;  // e.g. on retry after abandonBlock
        }
        if (last.getGenerationStamp() != commitBlock.getGenerationStamp()) {
            throw new IOException("Commit block with mismatching GS. NN has " + last + ", client submits "
                    + commitBlock);
        }
        last.setNumBytes(commitBlock.getNumBytes());
        last.getUnderConstructionFeature().setBlockUCState(BlockUCState.COMMITTED);
        Log.info(LOG, "BLOCK* commit " + last + " len=" + last.getNumBytes() + " -> COMMITTED");
        if (hasMinStorage(last)) {
            completeBlock(last);
        }
        return true;
    }

    public boolean hasMinStorage(BlockInfo b) {
        return countLiveReplicas(b) >= HdfsConfig.MIN_REPLICATION;
    }

    public void completeBlock(BlockInfo b) {
        if (!b.isComplete()) {
            if (countLiveReplicas(b) < b.getReplication()) {
                // addExpectedReplicasToPending: the remaining pipeline DataNodes will probably
                // report shortly via IBR - do not re-replicate immediately.
                pendingReconstruction.put(b, Time.monotonicNow());
            }
            b.convertToCompleteBlock();
            Log.info(LOG, "BLOCK* " + b + " -> COMPLETE (" + countLiveReplicas(b) + " replica(s))");
        }
    }

    /** Starts block recovery for the last block of a file (see internalReleaseLease). */
    public DatanodeDescriptor initializeBlockRecovery(BlockInfo b, long recoveryId) {
        return b.getUnderConstructionFeature().initializeBlockRecovery(b, recoveryId);
    }

    /** Applies the result of a block recovery (part of commitBlockSynchronization). */
    public void commitBlockSynchronization(BlockInfo stored, long newGs, long newLength,
                                           DatanodeID[] newTargets, String[] newTargetStorages, boolean closeFile) {
        Block oldReplica = new Block(stored);
        Set<DatanodeDescriptor> newNodes = new HashSet<>();
        List<DatanodeStorageInfo> newStorages = new ArrayList<>();
        for (int i = 0; i < newTargets.length; i++) {
            DatanodeDescriptor dn = datanodeManager.getDatanode(newTargets[i]);
            if (dn != null) {
                newNodes.add(dn);
                DatanodeStorageInfo s = newTargetStorages != null && i < newTargetStorages.length
                        ? dn.getStorageInfo(newTargetStorages[i]) : null;
                if (s != null) {
                    newStorages.add(s);
                }
            }
        }
        // Replicas with the old GS that did not take part in the recovery are stale
        for (DatanodeStorageInfo s : new ArrayList<>(stored.getStorageInfos())) {
            if (!newNodes.contains(s.getDatanodeDescriptor())) {
                s.removeBlock(stored);
                addToInvalidates(oldReplica, s.getDatanodeDescriptor());
            }
        }
        stored.setGenerationStamp(newGs);
        stored.setNumBytes(newLength);
        // As in the original: record the targets as locations right away (the IBRs may follow later)
        for (DatanodeStorageInfo s : newStorages) {
            if (s.getDatanodeDescriptor().isAlive()) {
                s.addBlock(stored);
            }
        }
        if (closeFile) {
            completeBlock(stored);
        }
    }

    // ============================================ Reports from DataNodes
    /**
     * A DataNode reports a FINALIZED replica on one of its storages (via IBR or block report).
     * Core of the replica bookkeeping.
     */
    private void addStoredBlock(DatanodeStorageInfo storage, Block reported) {
        DatanodeDescriptor node = storage.getDatanodeDescriptor();
        BlockInfo stored = blocksMap.getStoredBlock(reported);
        if (stored == null || stored.isDeleted()) {
            // Block does not belong to any file (anymore) -> have it deleted
            Log.info(LOG, "BLOCK* addStoredBlock: " + reported + " on " + node + " does not belong to any file"
                    + " -> invalidate");
            addToInvalidates(reported, node);
            return;
        }
        if (reported.getGenerationStamp() < stored.getGenerationStamp()) {
            // Stale replica (e.g. from a DataNode that did not take part in the recovery)
            Log.info(LOG, "BLOCK* addStoredBlock: stale replica " + reported + " on " + node + " (NN has " + stored
                    + ") -> invalidate");
            addToInvalidates(reported, node);
            return;
        }
        if (reported.getGenerationStamp() > stored.getGenerationStamp()) {
            // Replica was already finalized by a recovery, commitBlockSynchronization is about to arrive
            return;
        }
        if (stored.isCompleteOrCommitted() && reported.getNumBytes() != stored.getNumBytes()) {
            Log.info(LOG, "BLOCK* addStoredBlock: replica " + reported + " on " + node + " has wrong length "
                    + reported.getNumBytes() + " (expected " + stored.getNumBytes() + ") -> corrupt, invalidate");
            addToInvalidates(reported, node);
            return;
        }
        storage.addBlock(stored);
        if (stored.getBlockUCState() == BlockUCState.COMMITTED && hasMinStorage(stored)) {
            completeBlock(stored);
        }
        if (stored.isComplete()) {
            checkRedundancy(stored);
        }
    }

    /** Removes a replica from the bookkeeping (storage/DataNode gone, replica deleted/corrupt). */
    private void removeStoredBlock(BlockInfo b, DatanodeStorageInfo storage) {
        if (storage.removeBlock(b)) {
            if (b.isComplete() && !b.isDeleted()) {
                checkRedundancy(b);
            }
        }
    }

    private void removeStoredBlock(BlockInfo b, DatanodeDescriptor node) {
        DatanodeStorageInfo storage = b.findStorageInfo(node);
        if (storage != null) {
            removeStoredBlock(b, storage);
        }
    }

    public void processIncrementalBlockReport(DatanodeDescriptor node, StorageReceivedDeletedBlocks[] reports) {
        for (StorageReceivedDeletedBlocks r : reports) {
            // As in the original: an unknown storage (e.g. just added) is created
            DatanodeStorageInfo storage = node.updateStorage(r.getStorage());
            for (ReceivedDeletedBlockInfo info : r.getBlocks()) {
                switch (info.getStatus()) {
                    case RECEIVED_BLOCK:
                        Log.info(LOG, "BLOCK* IBR: " + node + " received " + info.getBlock() + " len="
                                + info.getBlock().getNumBytes() + " on " + storage.getStorageID());
                        addStoredBlock(storage, info.getBlock());
                        break;
                    case DELETED_BLOCK:
                        BlockInfo stored = blocksMap.getStoredBlock(info.getBlock());
                        if (stored != null) {
                            removeStoredBlock(stored, storage);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Full block report: compute the difference to the known state for each storage.
     * Storages missing from the report no longer exist on the DataNode (disk removed,
     * reformatted, failed): their replicas are removed ("zombie storages",
     * HDFS-7960) and the storage is removed (pruneStorageMap).
     */
    public void processReport(DatanodeDescriptor node, StorageBlockReport[] reports) {
        Set<DatanodeStorageInfo> reportedStorages = new HashSet<>();
        for (StorageBlockReport sbr : reports) {
            DatanodeStorageInfo storage = node.updateStorage(sbr.getStorage());
            reportedStorages.add(storage);
            processReport(storage, sbr.getBlocks());
        }
        for (DatanodeStorageInfo storage : new ArrayList<>(node.getStorageInfos())) {
            if (!reportedStorages.contains(storage)) {
                Log.info(LOG, "BLOCK* processReport: " + storage + " no longer reported -> removing it and its "
                        + storage.numBlocks() + " replica(s)");
                removeBlocksAssociatedTo(storage);
                node.removeStorage(storage);
            }
        }
    }

    private void processReport(DatanodeStorageInfo storage, BlockReportReplica[] report) {
        DatanodeDescriptor node = storage.getDatanodeDescriptor();
        Set<BlockInfo> notReported = new HashSet<>(storage.getBlocks());
        int finalized = 0;
        for (BlockReportReplica r : report) {
            Block reported = r.getBlock();
            BlockInfo stored = blocksMap.getStoredBlock(reported);
            if (stored != null) {
                notReported.remove(stored);
            }
            if (r.getState() == ReplicaState.FINALIZED) {
                finalized++;
                addStoredBlock(storage, reported);
            } else if (stored == null || stored.isDeleted()
                    || reported.getGenerationStamp() < stored.getGenerationStamp()) {
                addToInvalidates(reported, node);
            } else if (!stored.isComplete()) {
                // RBW/RWR of a block under construction: remember as expected location (important for recovery)
                stored.getUnderConstructionFeature().addReplicaIfNotPresent(storage);
            }
        }
        for (BlockInfo b : notReported) {
            removeStoredBlock(b, storage);
        }
        storage.receivedBlockReport();
        Log.info(LOG, "BLOCK* processReport from " + storage + ": " + report.length + " replicas (" + finalized
                + " finalized), " + notReported.size() + " removed");
    }

    /** Storage failed or disappeared: remove all of its replicas. */
    void removeBlocksAssociatedTo(DatanodeStorageInfo storage) {
        for (BlockInfo b : new ArrayList<>(storage.getBlocks())) {
            removeStoredBlock(b, storage);
        }
    }

    /** DataNode dead or re-registered: remove all of its replicas (on all storages). */
    void removeBlocksAssociatedTo(DatanodeDescriptor node) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
            removeBlocksAssociatedTo(storage);
        }
        node.resetBlocksAndQueues();
    }

    /** The client detected a checksum error while reading. */
    public void markBlockAsCorrupt(Block blk, DatanodeInfo dnInfo) {
        BlockInfo stored = blocksMap.getStoredBlock(blk);
        DatanodeDescriptor dn = datanodeManager.getDatanode(dnInfo);
        if (stored == null || dn == null || stored.findStorageInfo(dn) == null) {
            return;
        }
        if (countLiveReplicas(stored) <= 1) {
            // As in the original: the last replica is not deleted, even if it is corrupt
            Log.warn(LOG, "BLOCK* markBlockAsCorrupt: " + stored + " on " + dn + " is the only replica - keeping it");
            return;
        }
        Log.info(LOG, "BLOCK* markBlockAsCorrupt: " + stored + " on " + dn + " -> invalidate + re-replicate");
        removeStoredBlock(stored, dn);
        addToInvalidates(new Block(stored), dn);
    }

    // ============================================================ Deletion
    /** File deleted or block abandoned: delete the block from the BlocksMap and on all DataNodes. */
    public void removeBlock(BlockInfo block) {
        Set<DatanodeDescriptor> nodes = new LinkedHashSet<>(block.getDatanodes());
        if (block.getUnderConstructionFeature() != null) {
            nodes.addAll(block.getUnderConstructionFeature().getExpectedLocations());
        }
        for (DatanodeDescriptor dn : nodes) {
            addToInvalidates(new Block(block), dn);
        }
        neededReconstruction.remove(block);
        pendingReconstruction.remove(block);
        blocksMap.removeBlock(block);
    }

    private void addToInvalidates(Block b, DatanodeDescriptor dn) {
        dn.addBlockToBeInvalidated(new Block(b));
    }

    // ===================================================== Redundancy
    /** Replicas on healthy storages of live DataNodes. */
    public int countLiveReplicas(BlockInfo b) {
        int live = 0;
        for (DatanodeStorageInfo s : b.getStorageInfos()) {
            if (s.getState() == DatanodeStorage.State.NORMAL && s.getDatanodeDescriptor().isAlive()) {
                live++;
            }
        }
        return live;
    }

    /** Checks expected vs. actual replication of a complete block and enqueues it if necessary. */
    public void checkRedundancy(BlockInfo b) {
        if (b.isDeleted() || !b.isComplete()) {
            return;
        }
        INodeFile file = namesystem.getBlockCollection(b.getBlockCollectionId());
        if (file != null && file.isUnderConstruction() && b == file.getLastBlock()) {
            return;  // last block of an open file
        }
        int live = countLiveReplicas(b);
        int expected = b.getReplication();
        if (live < expected) {
            if (!pendingReconstruction.containsKey(b) && neededReconstruction.add(b)) {
                Log.info(LOG, "BLOCK* " + b + " is under-replicated (" + live + "/" + expected
                        + ") -> neededReconstruction");
            }
        } else {
            neededReconstruction.remove(b);
            pendingReconstruction.remove(b);
            if (live > expected) {
                processExtraRedundancy(b, live - expected);
            }
        }
    }

    private void processExtraRedundancy(BlockInfo b, int excess) {
        // Original: chooses by rack distribution, then the storage with the least free space;
        // here: the storage with the least free space
        List<DatanodeStorageInfo> storages = new ArrayList<>(b.getStorageInfos());
        storages.sort((x, y) -> Long.compare(x.getRemaining(), y.getRemaining()));
        for (int i = 0; i < excess; i++) {
            DatanodeStorageInfo s = storages.get(i);
            Log.info(LOG, "BLOCK* " + b + " is over-replicated -> delete replica on " + s);
            s.removeBlock(b);
            addToInvalidates(new Block(b), s.getDatanodeDescriptor());
        }
    }

    /** One pass of the RedundancyMonitor (computeDatanodeWork). */
    void computeDatanodeWork() {
        long now = Time.monotonicNow();
        // Reschedule timed-out commands
        Iterator<Map.Entry<BlockInfo, Long>> pit = pendingReconstruction.entrySet().iterator();
        while (pit.hasNext()) {
            Map.Entry<BlockInfo, Long> e = pit.next();
            if (now - e.getValue() > HdfsConfig.PENDING_RECONSTRUCTION_TIMEOUT_MS) {
                pit.remove();
                checkRedundancy(e.getKey());
            }
        }

        Iterator<BlockInfo> it = neededReconstruction.iterator();
        while (it.hasNext()) {
            BlockInfo b = it.next();
            int live = countLiveReplicas(b);
            if (b.isDeleted() || live >= b.getReplication()) {
                it.remove();
                continue;
            }
            if (live == 0) {
                continue;  // "missing block" - no replica left
            }
            // Source: live DataNode with the fewest pending commands
            DatanodeDescriptor source = null;
            for (DatanodeStorageInfo s : b.getStorageInfos()) {
                DatanodeDescriptor dn = s.getDatanodeDescriptor();
                if (s.getState() == DatanodeStorage.State.NORMAL && dn.isAlive() && (source == null
                        || dn.getNumberOfBlocksToBeReplicated() < source.getNumberOfBlocksToBeReplicated())) {
                    source = dn;
                }
            }
            Set<DatanodeID> excluded = new HashSet<>(b.getDatanodes());
            for (DatanodeDescriptor dn : datanodeManager.getDatanodes()) {
                if (dn.containsInvalidateBlock(b)) {
                    excluded.add(dn);  // a (corrupt/stale) copy is still there
                }
            }
            DatanodeStorageInfo[] targets = placementPolicy.chooseTarget(b.getReplication() - live,
                    datanodeManager.getDatanodes(), excluded, b.getNumBytes());
            if (targets.length == 0) {
                continue;  // currently no suitable DataNode
            }
            source.addBlockToBeReplicated(new Block(b), targets);
            pendingReconstruction.put(b, now);
            it.remove();
            Log.info(LOG, "BLOCK* ask " + source + " to replicate " + b + " to " + List.of(targets));
        }
    }

    public Set<BlockInfo> getNeededReconstruction() {
        return neededReconstruction;
    }

    public Set<BlockInfo> getPendingReconstruction() {
        return pendingReconstruction.keySet();
    }

    // ================================================== Background threads
    public void activate() {
        datanodeManager.startHeartbeatMonitor();
        running = true;
        redundancyThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(HdfsConfig.REDUNDANCY_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
                namesystem.writeLock();
                try {
                    computeDatanodeWork();
                } finally {
                    namesystem.writeUnlock();
                }
            }
        }, "RedundancyMonitor");
        redundancyThread.setDaemon(true);
        redundancyThread.start();
    }

    public void close() {
        running = false;
        if (redundancyThread != null) {
            redundancyThread.interrupt();
        }
        datanodeManager.stopHeartbeatMonitor();
    }
}
