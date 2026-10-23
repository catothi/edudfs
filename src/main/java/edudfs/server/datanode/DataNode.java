package edudfs.server.datanode;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.protocol.Block;
import edudfs.protocol.BlockLocalPathInfo;
import edudfs.protocol.ClientDatanodeProtocol;
import edudfs.protocol.DataTransferProtocol;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.PipelineAck;
import edudfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import edudfs.server.protocol.DatanodeProtocol;
import edudfs.server.protocol.InterDatanodeProtocol;
import edudfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import edudfs.server.protocol.ReplicaRecoveryInfo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.DataNode.
 *
 * Components:
 * <ul>
 *   <li>{@link FsDatasetImpl}: replicas on the local disks (volumes from dfs.datanode.data.dir)</li>
 *   <li>{@link DataXceiverServer}: data transfer (read/write/pipeline) over TCP</li>
 *   <li>{@link BPServiceActor}: heartbeats/block reports to the NameNode</li>
 *   <li>InterDatanodeProtocol (RMI, ipcPort): for block recovery</li>
 *   <li>ClientDatanodeProtocol (RMI, ipcPort): visible length of a replica being written</li>
 * </ul>
 *
 * EduDFS: all DataNodes run on the same machine and read the same hdfs-site.xml.
 * So that each one still has "its own disks", each gets a root directory (hostRoot)
 * that represents its file system. E.g. "/data/1/dfs/dn" thus becomes "data/dn2/data/1/dfs/dn".
 */
public class DataNode implements InterDatanodeProtocol, ClientDatanodeProtocol {

    private static final String LOG = "DataNode";

    private final DatanodeID id;
    private final FsDatasetImpl data;
    private final DataXceiverServer xserver;
    private final BPServiceActor actor;
    private final BlockRecoveryWorker blockRecoveryWorker = new BlockRecoveryWorker(this);
    private volatile boolean shouldRun = true;
    private Registry registry;
    private final Thread xserverThread;
    private final Thread actorThread;

    /** Volumes from the shared configuration (dfs.datanode.data.dir), resolved under hostRoot. */
    public DataNode(File hostRoot, int xferPort, int ipcPort, String nnHost, int nnPort) throws IOException {
        this(StorageLocation.parseAll(HdfsConfig.DATANODE_DATA_DIRS, hostRoot), xferPort, ipcPort, nnHost, nnPort);
    }

    public DataNode(List<StorageLocation> dataDirs, int xferPort, int ipcPort, String nnHost, int nnPort)
            throws IOException {
        this.id = new DatanodeID(loadOrCreateUuid(dataDirs), "localhost", xferPort, ipcPort);
        this.data = new FsDatasetImpl(dataDirs, id.getDatanodeUuid(), HdfsConfig.FAILED_VOLUMES_TOLERATED,
                toString());
        this.xserver = new DataXceiverServer(this, xferPort);
        this.actor = new BPServiceActor(this, nnHost, nnPort);
        this.xserverThread = new Thread(xserver, "DataXceiverServer@" + this);
        this.actorThread = new Thread(actor, "BPServiceActor@" + this);
        xserverThread.setDaemon(true);
        actorThread.setDaemon(true);
    }

    /**
     * As in the original, the UUID is stored in current/VERSION of EVERY volume (the same everywhere).
     * It is taken from the first readable volume, otherwise newly generated.
     */
    private static String loadOrCreateUuid(List<StorageLocation> dataDirs) {
        for (StorageLocation loc : dataDirs) {
            try {
                String uuid = FsVolumeImpl.loadVersion(loc.getDir()).getProperty("datanodeUuid");
                if (uuid != null) {
                    return uuid;
                }
            } catch (IOException e) {
                // broken volume - FsDatasetImpl counts it as failed
            }
        }
        return UUID.randomUUID().toString();
    }

    public void start() throws IOException {
        System.setProperty("java.rmi.server.hostname", id.getHostName());
        InterDatanodeProtocol stub = (InterDatanodeProtocol) UnicastRemoteObject.exportObject(this, 0);
        registry = LocateRegistry.createRegistry(id.getIpcPort());
        registry.rebind(InterDatanodeProtocol.NAME, stub);
        registry.rebind(ClientDatanodeProtocol.NAME, stub);
        xserverThread.start();
        actorThread.start();
        Log.info(LOG, this + " started (uuid " + id.getDatanodeUuid() + ", " + data.getVolumes().size()
                + " volume(s), " + data.getNumBlocks() + " replicas on disk)");
    }

    /** Stops the DataNode abruptly (like a process kill): connections are simply closed. */
    public void shutdown() {
        shouldRun = false;
        xserver.kill();
        actorThread.interrupt();
        try {
            UnicastRemoteObject.unexportObject(this, true);
            UnicastRemoteObject.unexportObject(registry, true);
        } catch (IOException ignored) {
            // ignore
        }
        Log.info(LOG, this + " shut down");
    }

    boolean isRunning() {
        return shouldRun;
    }

    public DatanodeID getDatanodeId() {
        return id;
    }

    public FsDatasetImpl getFSDataset() {
        return data;
    }

    DataXceiverServer getXceiverServer() {
        return xserver;
    }

    DatanodeProtocol getNamenode() {
        return actor.getNamenode();
    }

    BlockRecoveryWorker getBlockRecoveryWorker() {
        return blockRecoveryWorker;
    }

    /** Path of the block file - only for demos (e.g. to deliberately corrupt a replica). */
    public File getBlockFile(long blockId) {
        ReplicaInfo r = data.getReplica(blockId);
        return r == null ? null : data.getBlockFile(r);
    }

    void notifyNamenodeReceivedBlock(ReplicaInfo r) {
        actor.notifyNamenodeBlock(r.toBlock(), BlockStatus.RECEIVED_BLOCK, r.getVolume().toDatanodeStorage());
    }

    /**
     * Corresponds to DataNode.checkDiskError() + handleVolumeFailures(): remove broken volumes.
     * If more than dfs.datanode.failed.volumes.tolerated have failed, the DataNode shuts down;
     * otherwise it immediately reports with a full block report (scheduleAllBlockReport(0)).
     * The NameNode already learns that a storage is missing with the next heartbeat
     * (fewer StorageReports, higher number of failed volumes).
     */
    void checkDiskError() {
        List<FsVolumeImpl> failed = data.checkDataDir();
        if (failed.isEmpty()) {
            return;
        }
        if (!data.hasEnoughResource()) {
            Log.warn(LOG, this + ": too many failed volumes (" + data.getNumFailedVolumes() + " > tolerated "
                    + HdfsConfig.FAILED_VOLUMES_TOLERATED + ") - shutting down DataNode");
            shutdown();
            return;
        }
        actor.scheduleBlockReport();
    }

    InterDatanodeProtocol getInterDatanodeProxy(DatanodeID dn) throws IOException {
        if (dn.equals(id)) {
            return this;
        }
        try {
            return (InterDatanodeProtocol) LocateRegistry.getRegistry(dn.getHostName(), dn.getIpcPort())
                    .lookup(InterDatanodeProtocol.NAME);
        } catch (NotBoundException e) {
            throw new IOException(e);
        }
    }

    // ==================================================== InterDatanodeProtocol
    @Override
    public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock) throws IOException {
        return data.initReplicaRecovery(rBlock);
    }

    @Override
    public String updateReplicaUnderRecovery(Block oldBlock, long recoveryId, long newLength) throws IOException {
        ReplicaInfo r = data.updateReplicaUnderRecovery(oldBlock, recoveryId, newLength);
        notifyNamenodeReceivedBlock(r);
        return r.getVolume().getStorageID();  // as in the original: the storageID of the replica
    }

    // =================================================== ClientDatanodeProtocol
    @Override
    public long getReplicaVisibleLength(Block b) throws IOException {
        ReplicaInfo r = data.getReplica(b.getBlockId());
        if (r == null) {
            throw new IOException("Replica not found for " + b);
        }
        return r.getNumBytes();
    }

    @Override
    public BlockLocalPathInfo getBlockLocalPathInfo(Block b) throws IOException {
        ReplicaInfo r = data.getReplica(b.getBlockId());
        if (r == null) {
            throw new IOException("Replica not found for " + b);
        }
        return new BlockLocalPathInfo(r.toBlock(), data.getBlockFile(r).getPath(), data.getMetaFile(r).getPath());
    }

    // ============================================================ DNA_TRANSFER
    /**
     * Re-replication (DataNode.DataTransfer): this DataNode acts like a client
     * and writes the block to the targets via an OP_WRITE_BLOCK pipeline.
     */
    void transferBlock(Block block, DatanodeInfo[] targets) {
        Thread t = new Thread(() -> {
            try (BlockSender sender = new BlockSender(data, block, 0, Long.MAX_VALUE);
                 Socket s = DataTransferProtocol.connect(targets[0])) {
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 128 * 1024));
                DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                DataTransferProtocol.sendWriteBlock(out, sender.getBlock(), "",
                        Arrays.copyOfRange(targets, 1, targets.length));
                short status = in.readShort();
                String firstBadLink = in.readUTF();
                if (status != DataTransferProtocol.SUCCESS) {
                    throw new IOException("pipeline setup failed, firstBadLink=" + firstBadLink);
                }
                sender.sendBlock(out);
                PipelineAck ack;
                do {
                    ack = PipelineAck.read(in);
                } while (ack.seqno != sender.getLastSeqno());
                Log.info(LOG, this + ": transferred " + block + " to " + Arrays.toString(targets));
            } catch (IOException e) {
                Log.warn(LOG, this + ": failed to transfer " + block + " to " + Arrays.toString(targets) + ": "
                        + e.getMessage());
            }
        }, "DataTransfer@" + this);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public String toString() {
        return "DN(" + id.getXferAddr() + ")";
    }

    /** Usage: DataNode &lt;hostRoot&gt; &lt;xferPort&gt; &lt;ipcPort&gt; */
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DataNode <hostRoot> <xferPort> <ipcPort>");
            System.err.println("  hostRoot = root of the simulated machine; the volumes from dfs.datanode.data.dir"
                    + " (" + HdfsConfig.CONF_DIR + "/hdfs-site.xml) are located below it");
            System.exit(1);
        }
        DataNode dn = new DataNode(new File(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]),
                HdfsConfig.NAMENODE_HOST, HdfsConfig.NAMENODE_RPC_PORT);
        dn.start();
        Runtime.getRuntime().addShutdownHook(new Thread(dn::shutdown));
        Thread.currentThread().join();
    }
}
