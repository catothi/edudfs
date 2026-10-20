package edudfs.server.blockmanagement;

import edudfs.common.Time;
import edudfs.server.common.HdfsServerConstants.BlockUCState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature.
 *
 * While a block is being written, the NameNode does not yet know any FINALIZED replicas.
 * It therefore remembers the "expected" locations = the pipeline it handed out in
 * addBlock(). This list is needed during lease recovery.
 *
 * The expected locations are storages. Which disk a DataNode actually uses is decided
 * by the DataNode itself (VolumeChoosingPolicy); if it reports the replica from a different
 * storage, the entry for that DataNode is replaced.
 */
public class BlockUnderConstructionFeature {

    private BlockUCState blockUCState;
    private List<DatanodeStorageInfo> replicas;  // in the original: ReplicaUnderConstruction[]
    private long blockRecoveryId = 0;
    private long recoveryStartTime = 0;
    private DatanodeDescriptor primary;

    BlockUnderConstructionFeature(BlockUCState state, DatanodeStorageInfo[] targets) {
        this.blockUCState = state;
        setExpectedLocations(targets);
    }

    public BlockUCState getBlockUCState() {
        return blockUCState;
    }

    void setBlockUCState(BlockUCState s) {
        this.blockUCState = s;
    }

    void setExpectedLocations(DatanodeStorageInfo[] targets) {
        replicas = new ArrayList<>(Arrays.asList(targets));
    }

    public List<DatanodeStorageInfo> getExpectedStorageLocations() {
        return Collections.unmodifiableList(replicas);
    }

    /** The DataNodes of the expected locations. */
    public List<DatanodeDescriptor> getExpectedLocations() {
        List<DatanodeDescriptor> r = new ArrayList<>(replicas.size());
        for (DatanodeStorageInfo s : replicas) {
            r.add(s.getDatanodeDescriptor());
        }
        return r;
    }

    public int getNumExpectedLocations() {
        return replicas.size();
    }

    void addReplicaIfNotPresent(DatanodeStorageInfo storage) {
        if (replicas.contains(storage)) {
            return;
        }
        // Same DataNode, different disk -> replace entry
        replicas.removeIf(s -> s.getDatanodeDescriptor() == storage.getDatanodeDescriptor());
        replicas.add(storage);
    }

    public long getBlockRecoveryId() {
        return blockRecoveryId;
    }

    public long getRecoveryStartTime() {
        return recoveryStartTime;
    }

    public DatanodeDescriptor getPrimary() {
        return primary;
    }

    /**
     * Starts block recovery: state UNDER_RECOVERY, new recoveryId (= future GS)
     * and selection of a primary DataNode. As in the original, the live DataNode
     * with the most recent heartbeat is chosen. The command is delivered with the
     * next heartbeat of that DataNode.
     */
    DatanodeDescriptor initializeBlockRecovery(BlockInfo block, long recoveryId) {
        setBlockUCState(BlockUCState.UNDER_RECOVERY);
        blockRecoveryId = recoveryId;
        recoveryStartTime = Time.monotonicNow();
        primary = null;
        for (DatanodeStorageInfo s : replicas) {
            DatanodeDescriptor dn = s.getDatanodeDescriptor();
            if (dn.isAlive() && (primary == null || dn.getLastUpdateMonotonic() > primary.getLastUpdateMonotonic())) {
                primary = dn;
            }
        }
        if (primary != null) {
            primary.addBlockToBeRecovered(block);
        }
        return primary;
    }
}
