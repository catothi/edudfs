package edudfs.server.datanode;

import edudfs.common.Log;
import edudfs.protocol.Block;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.server.common.HdfsServerConstants.ReplicaState;
import edudfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import edudfs.server.protocol.InterDatanodeProtocol;
import edudfs.server.protocol.ReplicaRecoveryInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.BlockRecoveryWorker.
 *
 * The primary DataNode of a block recovery:
 * <ol>
 *   <li>asks all participating DataNodes for their replica via initReplicaRecovery()
 *       (this stops any writers that may still be running),</li>
 *   <li>determines the common length (syncBlock): the FINALIZED length if there is one,
 *       otherwise the MINIMUM length of all RBW (or RWR) replicas - all of them are
 *       guaranteed to have these bytes,</li>
 *   <li>has all replicas truncated to this length and finalized with a new GS,</li>
 *   <li>reports the result to the NameNode via commitBlockSynchronization().</li>
 * </ol>
 */
class BlockRecoveryWorker {

    private static final String LOG = "BlockRecoveryWorker";

    private final DataNode datanode;

    BlockRecoveryWorker(DataNode datanode) {
        this.datanode = datanode;
    }

    private static class BlockRecord {
        final DatanodeID id;
        final InterDatanodeProtocol datanode;
        final ReplicaRecoveryInfo rInfo;

        BlockRecord(DatanodeID id, InterDatanodeProtocol datanode, ReplicaRecoveryInfo rInfo) {
            this.id = id;
            this.datanode = datanode;
            this.rInfo = rInfo;
        }
    }

    void recoverBlocks(List<RecoveringBlock> blocks) {
        Thread t = new Thread(() -> {
            for (RecoveringBlock b : blocks) {
                try {
                    recoverBlock(b);
                } catch (IOException e) {
                    Log.warn(LOG, datanode + ": recovery of " + b.getBlock() + " failed: " + e.getMessage());
                }
            }
        }, "BlockRecoveryWorker@" + datanode);
        t.setDaemon(true);
        t.start();
    }

    private void recoverBlock(RecoveringBlock rBlock) throws IOException {
        Block block = rBlock.getBlock();
        Log.info(LOG, datanode + ": I am primary for recovery of " + rBlock);
        List<BlockRecord> syncList = new ArrayList<>();
        for (DatanodeInfo id : rBlock.getLocations()) {
            try {
                InterDatanodeProtocol proxy = datanode.getInterDatanodeProxy(id);
                ReplicaRecoveryInfo info = proxy.initReplicaRecovery(rBlock);
                if (info != null && info.getGenerationStamp() >= block.getGenerationStamp()) {
                    syncList.add(new BlockRecord(id, proxy, info));
                    Log.info(LOG, "  replica on " + id + ": len=" + info.getNumBytes() + ", state="
                            + info.getOriginalReplicaState());
                } else {
                    Log.info(LOG, "  no replica on " + id);
                }
            } catch (Exception e) {
                Log.info(LOG, "  failed to contact " + id + ": " + e.getMessage());
            }
        }
        syncBlock(rBlock, syncList);
    }

    private void syncBlock(RecoveringBlock rBlock, List<BlockRecord> syncList) throws IOException {
        Block block = rBlock.getBlock();
        long recoveryId = rBlock.getNewGenerationStamp();
        if (syncList.isEmpty()) {
            // No DataNode has the block -> remove the block from the file
            datanode.getNamenode().commitBlockSynchronization(block, recoveryId, 0, true, true, new DatanodeID[0],
                    new String[0]);
            return;
        }

        // Determine the best state: FINALIZED > RBW > RWR
        ReplicaState bestState = ReplicaState.RWR;
        for (BlockRecord r : syncList) {
            ReplicaState s = r.rInfo.getOriginalReplicaState();
            if (s == ReplicaState.FINALIZED || s == ReplicaState.RBW && bestState == ReplicaState.RWR) {
                bestState = s;
            }
        }
        long newLength = Long.MAX_VALUE;
        List<BlockRecord> participants = new ArrayList<>();
        if (bestState == ReplicaState.FINALIZED) {
            for (BlockRecord r : syncList) {
                if (r.rInfo.getOriginalReplicaState() == ReplicaState.FINALIZED) {
                    newLength = r.rInfo.getNumBytes();
                }
            }
            for (BlockRecord r : syncList) {
                ReplicaState s = r.rInfo.getOriginalReplicaState();
                if (s == ReplicaState.FINALIZED || s == ReplicaState.RBW && r.rInfo.getNumBytes() >= newLength) {
                    participants.add(r);
                }
            }
        } else {
            for (BlockRecord r : syncList) {
                if (r.rInfo.getOriginalReplicaState() == bestState) {
                    participants.add(r);
                    newLength = Math.min(newLength, r.rInfo.getNumBytes());
                }
            }
        }
        Log.info(LOG, datanode + ": syncBlock " + block + ": best state=" + bestState + ", new length=" + newLength
                + ", participants=" + participants.size());

        List<DatanodeID> successList = new ArrayList<>();
        List<String> successStorages = new ArrayList<>();
        for (BlockRecord r : participants) {
            try {
                successStorages.add(r.datanode.updateReplicaUnderRecovery(block, recoveryId, newLength));
                successList.add(r.id);
            } catch (IOException e) {
                Log.warn(LOG, "  updateReplicaUnderRecovery failed on " + r.id + ": " + e.getMessage());
            }
        }
        if (successList.isEmpty()) {
            throw new IOException("Cannot recover " + block + ": none of the participants could be updated");
        }
        datanode.getNamenode().commitBlockSynchronization(block, recoveryId, newLength, true, false,
                successList.toArray(new DatanodeID[0]), successStorages.toArray(new String[0]));
    }
}
