package edudfs.server.datanode;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.common.Time;
import edudfs.protocol.Block;
import edudfs.server.common.HdfsServerConstants;
import edudfs.server.protocol.BlockCommand;
import edudfs.server.protocol.BlockRecoveryCommand;
import edudfs.server.protocol.DatanodeCommand;
import edudfs.server.protocol.DatanodeProtocol;
import edudfs.server.protocol.DatanodeStorage;
import edudfs.server.protocol.ReceivedDeletedBlockInfo;
import edudfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import edudfs.server.protocol.StorageReceivedDeletedBlocks;

import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.BPServiceActor:
 * the thread that talks to the NameNode.
 * <ul>
 *   <li>registration + initial full block report</li>
 *   <li>heartbeats with one StorageReport per volume; their response contains commands
 *       (transfer, invalidate, recovery)</li>
 *   <li>incremental block reports (IBR) - immediately, as soon as a block was received/deleted</li>
 *   <li>periodic full block reports (one list per storage)</li>
 * </ul>
 * Simplification: one BPServiceActor for the single block pool/NameNode (original: one per
 * NameNode, bundled in the BPOfferService).
 */
class BPServiceActor implements Runnable {

    private static final String LOG = "BPServiceActor";

    private final DataNode dn;
    private final String nnHost;
    private final int nnPort;
    private DatanodeProtocol bpNamenode;

    /** Pending IBR entries per storage (original: IncrementalBlockReportManager). */
    private final Map<DatanodeStorage, List<ReceivedDeletedBlockInfo>> pendingIncrementalBR = new LinkedHashMap<>();
    private long lastHeartbeat = 0;
    private long lastBlockReport = 0;
    private volatile boolean forceFullBlockReport = false;
    private boolean needRegistration = true;

    BPServiceActor(DataNode dn, String nnHost, int nnPort) {
        this.dn = dn;
        this.nnHost = nnHost;
        this.nnPort = nnPort;
    }

    DatanodeProtocol getNamenode() {
        return bpNamenode;
    }

    void notifyNamenodeBlock(Block b, BlockStatus status, DatanodeStorage storage) {
        synchronized (pendingIncrementalBR) {
            pendingIncrementalBR.computeIfAbsent(storage, k -> new ArrayList<>())
                    .add(new ReceivedDeletedBlockInfo(b, status));
            pendingIncrementalBR.notifyAll();  // send the IBR immediately
        }
    }

    /** Send the next full block report immediately (original: scheduleBlockReport(0)). */
    void scheduleBlockReport() {
        forceFullBlockReport = true;
    }

    @Override
    public void run() {
        while (dn.isRunning()) {
            try {
                if (bpNamenode == null) {
                    bpNamenode = (DatanodeProtocol) LocateRegistry.getRegistry(nnHost, nnPort)
                            .lookup(DatanodeProtocol.NAME);
                }
                if (needRegistration) {
                    register();
                }
                offerService();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (!dn.isRunning()) {
                    return;
                }
                Log.warn(LOG, dn + ": problem talking to NameNode: " + e.getMessage());
                bpNamenode = null;
                try {
                    Thread.sleep(HdfsConfig.HEARTBEAT_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    return;
                }
            }
        }
    }

    private void register() throws Exception {
        bpNamenode.registerDatanode(dn.getDatanodeId());
        Log.info(LOG, dn + ": registered with NameNode " + nnHost + ":" + nnPort);
        needRegistration = false;
        synchronized (pendingIncrementalBR) {
            pendingIncrementalBR.clear();  // the full block report contains everything anyway
        }
        // As in the original: first a heartbeat (reports the storages including capacity), then the block report
        heartbeat();
        if (dn.isRunning()) {
            blockReport();
        }
    }

    private void heartbeat() throws Exception {
        lastHeartbeat = Time.monotonicNow();
        dn.checkDiskError();
        if (!dn.isRunning()) {
            return;
        }
        DatanodeCommand[] cmds = bpNamenode.sendHeartbeat(dn.getDatanodeId(),
                dn.getFSDataset().getStorageReports(), dn.getFSDataset().getNumFailedVolumes());
        for (DatanodeCommand cmd : cmds) {
            processCommand(cmd);
        }
    }

    private void blockReport() throws Exception {
        DatanodeCommand cmd = bpNamenode.blockReport(dn.getDatanodeId(), HdfsServerConstants.BLOCK_POOL_ID,
                dn.getFSDataset().getBlockReport());
        lastBlockReport = Time.monotonicNow();
        forceFullBlockReport = false;
        if (cmd != null) {
            processCommand(cmd);
        }
    }

    private void offerService() throws Exception {
        while (dn.isRunning() && !needRegistration) {
            long now = Time.monotonicNow();
            if (now - lastHeartbeat >= HdfsConfig.HEARTBEAT_INTERVAL_MS) {
                heartbeat();
            }
            if (!dn.isRunning() || needRegistration) {
                return;
            }
            reportReceivedDeletedBlocks();
            if (forceFullBlockReport || now - lastBlockReport >= HdfsConfig.BLOCK_REPORT_INTERVAL_MS) {
                blockReport();
            }
            long waitTime = HdfsConfig.HEARTBEAT_INTERVAL_MS - (Time.monotonicNow() - lastHeartbeat);
            synchronized (pendingIncrementalBR) {
                if (waitTime > 0 && pendingIncrementalBR.isEmpty()) {
                    pendingIncrementalBR.wait(waitTime);
                }
            }
        }
    }

    private void reportReceivedDeletedBlocks() throws Exception {
        List<StorageReceivedDeletedBlocks> report = new ArrayList<>();
        synchronized (pendingIncrementalBR) {
            if (pendingIncrementalBR.isEmpty()) {
                return;
            }
            for (Map.Entry<DatanodeStorage, List<ReceivedDeletedBlockInfo>> e : pendingIncrementalBR.entrySet()) {
                report.add(new StorageReceivedDeletedBlocks(e.getKey(),
                        e.getValue().toArray(new ReceivedDeletedBlockInfo[0])));
            }
            pendingIncrementalBR.clear();
        }
        try {
            bpNamenode.blockReceivedAndDeleted(dn.getDatanodeId(), HdfsServerConstants.BLOCK_POOL_ID,
                    report.toArray(new StorageReceivedDeletedBlocks[0]));
        } catch (Exception e) {
            synchronized (pendingIncrementalBR) {
                for (StorageReceivedDeletedBlocks r : report) {  // retry later
                    pendingIncrementalBR.computeIfAbsent(r.getStorage(), k -> new ArrayList<>())
                            .addAll(0, List.of(r.getBlocks()));
                }
            }
            throw e;
        }
    }

    private void processCommand(DatanodeCommand cmd) {
        switch (cmd.getAction()) {
            case DatanodeProtocol.DNA_REGISTER:
                Log.info(LOG, dn + ": NameNode requested re-registration");
                needRegistration = true;
                break;
            case DatanodeProtocol.DNA_INVALIDATE:
                for (ReplicaInfo r : dn.getFSDataset().invalidate(((BlockCommand) cmd).getBlocks())) {
                    notifyNamenodeBlock(r.toBlock(), BlockStatus.DELETED_BLOCK, r.getVolume().toDatanodeStorage());
                }
                break;
            case DatanodeProtocol.DNA_TRANSFER:
                BlockCommand bc = (BlockCommand) cmd;
                for (int i = 0; i < bc.getBlocks().length; i++) {
                    dn.transferBlock(bc.getBlocks()[i], bc.getTargets()[i]);
                }
                break;
            case DatanodeProtocol.DNA_RECOVERBLOCK:
                dn.getBlockRecoveryWorker().recoverBlocks(((BlockRecoveryCommand) cmd).getRecoveringBlocks());
                break;
            default:
                Log.warn(LOG, dn + ": unknown command " + cmd.getAction());
        }
    }
}
