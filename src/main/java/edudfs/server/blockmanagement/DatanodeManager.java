package edudfs.server.blockmanagement;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.common.Time;
import edudfs.protocol.Block;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.server.common.HdfsServerConstants.BlockUCState;
import edudfs.server.namenode.FSNamesystem;
import edudfs.server.protocol.BlockCommand;
import edudfs.server.protocol.BlockRecoveryCommand;
import edudfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import edudfs.server.protocol.DatanodeCommand;
import edudfs.server.protocol.DatanodeProtocol;
import edudfs.server.protocol.StorageReport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Corresponds to DatanodeManager + HeartbeatManager.
 * Manages registration, heartbeats and the detection of dead DataNodes.
 */
public class DatanodeManager {

    private static final String LOG = "DatanodeManager";

    private final FSNamesystem namesystem;
    private final BlockManager blockManager;
    /** datanodeUuid -> Descriptor. */
    private final Map<String, DatanodeDescriptor> datanodeMap = new TreeMap<>();

    private volatile boolean running;
    private Thread heartbeatMonitor;

    DatanodeManager(FSNamesystem namesystem, BlockManager blockManager) {
        this.namesystem = namesystem;
        this.blockManager = blockManager;
    }

    public DatanodeDescriptor getDatanode(DatanodeID id) {
        return id == null ? null : datanodeMap.get(id.getDatanodeUuid());
    }

    public Collection<DatanodeDescriptor> getDatanodes() {
        return datanodeMap.values();
    }

    public List<DatanodeDescriptor> getLiveDatanodes() {
        List<DatanodeDescriptor> live = new ArrayList<>();
        for (DatanodeDescriptor dn : datanodeMap.values()) {
            if (dn.isAlive()) {
                live.add(dn);
            }
        }
        return live;
    }

    public DatanodeInfo[] getDatanodeReport() {
        List<DatanodeInfo> r = new ArrayList<>();
        for (DatanodeDescriptor dn : datanodeMap.values()) {
            r.add(new DatanodeInfo(dn));  // copy: capacity, usage, blocks across all storages
        }
        return r.toArray(new DatanodeInfo[0]);
    }

    /** Called under the FSNamesystem write lock. */
    public void registerDatanode(DatanodeID reg) {
        DatanodeDescriptor dn = datanodeMap.get(reg.getDatanodeUuid());
        if (dn == null) {
            dn = new DatanodeDescriptor(reg);
            datanodeMap.put(reg.getDatanodeUuid(), dn);
            Log.info(LOG, "BLOCK* registerDatanode: new node " + reg.getXferAddr());
        } else {
            // Re-registration (restart or after being declared "dead"): discard the old block
            // mapping; the subsequent full block report rebuilds it.
            blockManager.removeBlocksAssociatedTo(dn);
            dn.updateRegInfo(reg);
            Log.info(LOG, "BLOCK* registerDatanode: re-registered node " + reg.getXferAddr());
        }
        dn.setAlive(true);
        dn.setLastUpdate(Time.now(), Time.monotonicNow());
    }

    /**
     * Processes a heartbeat and returns the accumulated commands.
     * Called under the FSNamesystem write lock.
     */
    public DatanodeCommand[] handleHeartbeat(DatanodeID reg, StorageReport[] reports, int failedVolumes) {
        DatanodeDescriptor dn = getDatanode(reg);
        if (dn == null || !dn.isAlive()) {
            // Unknown or already declared dead -> make it re-register
            return new DatanodeCommand[]{new DatanodeCommand(DatanodeProtocol.DNA_REGISTER)};
        }
        dn.setLastUpdate(Time.now(), Time.monotonicNow());
        for (DatanodeStorageInfo failed : dn.updateHeartbeatState(reports, failedVolumes)) {
            // Original: done by the HeartbeatManager monitor shortly afterwards (heartbeatCheck -> failedStorage)
            Log.warn(LOG, "BLOCK* handleHeartbeat: storage " + failed + " FAILED (" + failedVolumes
                    + " failed volume(s) on " + dn.getXferAddr() + ") -> removing its " + failed.numBlocks()
                    + " replica(s)");
            blockManager.removeBlocksAssociatedTo(failed);
        }

        List<DatanodeCommand> cmds = new ArrayList<>();

        // 1) Lease/block recovery
        List<RecoveringBlock> rbs = new ArrayList<>();
        for (BlockInfo b : dn.getLeaseRecoveryCommand(Integer.MAX_VALUE)) {
            BlockUnderConstructionFeature uc = b.getUnderConstructionFeature();
            if (uc == null || uc.getBlockUCState() != BlockUCState.UNDER_RECOVERY) {
                continue;  // already done in the meantime
            }
            rbs.add(new RecoveringBlock(new Block(b), BlockManager.toDatanodeInfos(uc.getExpectedLocations()),
                    uc.getBlockRecoveryId()));
        }
        if (!rbs.isEmpty()) {
            cmds.add(new BlockRecoveryCommand(rbs));
        }

        // 2) Re-replication
        List<DatanodeDescriptor.BlockTargetPair> transfers =
                dn.getReplicationCommand(HdfsConfig.MAX_REPLICATION_STREAMS);
        if (!transfers.isEmpty()) {
            Block[] blocks = new Block[transfers.size()];
            DatanodeInfo[][] targets = new DatanodeInfo[transfers.size()][];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = transfers.get(i).block;
                List<DatanodeDescriptor> nodes = new ArrayList<>();
                for (DatanodeStorageInfo t : transfers.get(i).targets) {
                    nodes.add(t.getDatanodeDescriptor());
                }
                targets[i] = BlockManager.toDatanodeInfos(nodes);
            }
            cmds.add(new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocks, targets));
        }

        // 3) Deletion
        List<Block> invalidate = dn.getInvalidateBlocks(HdfsConfig.BLOCK_INVALIDATE_LIMIT);
        if (!invalidate.isEmpty()) {
            cmds.add(new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, invalidate.toArray(new Block[0]), null));
        }
        return cmds.toArray(new DatanodeCommand[0]);
    }

    /** HeartbeatManager.heartbeatCheck(): declare DataNodes without heartbeat dead. */
    void heartbeatCheck() {
        long now = Time.monotonicNow();
        for (DatanodeDescriptor dn : datanodeMap.values()) {
            if (dn.isAlive() && now - dn.getLastUpdateMonotonic() > HdfsConfig.HEARTBEAT_EXPIRE_MS) {
                Log.info(LOG, "BLOCK* heartbeatCheck: lost heartbeat from " + dn.getXferAddr()
                        + " -> node is DEAD, removing its " + dn.getNumBlocks() + " block replica(s)");
                dn.setAlive(false);
                blockManager.removeBlocksAssociatedTo(dn);
            }
        }
    }

    void startHeartbeatMonitor() {
        running = true;
        heartbeatMonitor = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(HdfsConfig.HEARTBEAT_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
                namesystem.writeLock();
                try {
                    heartbeatCheck();
                } finally {
                    namesystem.writeUnlock();
                }
            }
        }, "HeartbeatManager.Monitor");
        heartbeatMonitor.setDaemon(true);
        heartbeatMonitor.start();
    }

    void stopHeartbeatMonitor() {
        running = false;
        if (heartbeatMonitor != null) {
            heartbeatMonitor.interrupt();
        }
    }
}
