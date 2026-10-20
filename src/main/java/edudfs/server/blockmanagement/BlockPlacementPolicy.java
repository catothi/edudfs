package edudfs.server.blockmanagement;

import edudfs.protocol.DatanodeID;
import edudfs.protocol.StorageType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Heavily simplified variant of BlockPlacementPolicyDefault.
 *
 * Original (rack-aware): 1st replica on the writer's node, 2nd on a different rack,
 * 3rd on the same rack as the 2nd - plus consideration of load, disk space and
 * storage policy (which StorageTypes are desired).
 * Here: random choice among the live, non-excluded DataNodes; for each DataNode
 * a healthy DISK storage with enough space for one block is chosen.
 */
public class BlockPlacementPolicy {

    public DatanodeStorageInfo[] chooseTarget(int numOfReplicas, Collection<DatanodeDescriptor> candidates,
                                              Set<? extends DatanodeID> excluded, long blockSize) {
        List<DatanodeDescriptor> usable = new ArrayList<>();
        for (DatanodeDescriptor dn : candidates) {
            if (dn.isAlive() && !excluded.contains(dn)) {
                usable.add(dn);
            }
        }
        Collections.shuffle(usable);
        List<DatanodeStorageInfo> result = new ArrayList<>();
        for (DatanodeDescriptor dn : usable) {
            if (result.size() >= numOfReplicas) {
                break;
            }
            DatanodeStorageInfo storage = dn.chooseStorage4Block(StorageType.DEFAULT, blockSize);
            if (storage != null) {
                result.add(storage);
            }
        }
        return result.toArray(new DatanodeStorageInfo[0]);
    }
}
