package edudfs.client;

import edudfs.common.HdfsConfig;
import edudfs.protocol.Block;
import edudfs.protocol.BlockLocalPathInfo;
import edudfs.protocol.ClientDatanodeProtocol;
import edudfs.protocol.ClientProtocol;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LastBlockWithStatus;
import edudfs.protocol.LocatedBlock;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Corresponds to org.apache.hadoop.hdfs.DFSClient.
 *
 * Metadata operations go to the NameNode via RPC; data flows directly
 * between the client and the DataNodes (DFSInputStream / DFSOutputStream).
 */
public class DFSClient implements Closeable {

    final ClientProtocol namenode;
    final String clientName;
    private final LeaseRenewer leaseRenewer;
    /** Open write streams (fileId -> stream); as long as this is not empty, the lease is renewed. */
    private final Map<Long, DFSOutputStream> filesBeingWritten = new HashMap<>();
    private volatile boolean clientRunning = true;

    public DFSClient() throws IOException {
        this(HdfsConfig.NAMENODE_HOST, HdfsConfig.NAMENODE_RPC_PORT);
    }

    public DFSClient(String nnHost, int nnPort) throws IOException {
        try {
            this.namenode = (ClientProtocol) LocateRegistry.getRegistry(nnHost, nnPort).lookup(ClientProtocol.NAME);
        } catch (NotBoundException e) {
            throw new IOException(e);
        }
        // Naming scheme as in the original
        this.clientName = "DFSClient_NONMAPREDUCE_" + ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE)
                + "_" + Thread.currentThread().threadId();
        this.leaseRenewer = new LeaseRenewer(this);
    }

    public String getClientName() {
        return clientName;
    }

    public ClientProtocol getNamenode() {
        return namenode;
    }

    private void checkOpen() throws IOException {
        if (!clientRunning) {
            throw new IOException("Filesystem closed");
        }
    }

    // -------------------------------------------------------------- Files
    public DFSOutputStream create(String src, boolean overwrite) throws IOException {
        return create(src, overwrite, HdfsConfig.DEFAULT_REPLICATION, HdfsConfig.DEFAULT_BLOCK_SIZE);
    }

    public DFSOutputStream create(String src, boolean overwrite, short replication, long blockSize)
            throws IOException {
        checkOpen();
        HdfsFileStatus stat = namenode.create(src, clientName, overwrite, replication, blockSize);
        DFSOutputStream out = new DFSOutputStream(this, src, stat);
        beginFileLease(stat.getFileId(), out);
        return out;
    }

    /** Appends to a closed file (new data starts in a new block). */
    public DFSOutputStream append(String src) throws IOException {
        checkOpen();
        LastBlockWithStatus lbs = namenode.append(src, clientName);
        HdfsFileStatus stat = lbs.getFileStatus();
        Block previous = lbs.getLastBlock() == null ? null : lbs.getLastBlock().getBlock();
        DFSOutputStream out = new DFSOutputStream(this, src, stat, previous);
        beginFileLease(stat.getFileId(), out);
        return out;
    }

    public DFSInputStream open(String src) throws IOException {
        checkOpen();
        return new DFSInputStream(this, src);
    }

    // ---------------------------------------------------------- Namespace
    public boolean mkdirs(String src) throws IOException {
        checkOpen();
        return namenode.mkdirs(src);
    }

    public boolean delete(String src, boolean recursive) throws IOException {
        checkOpen();
        return namenode.delete(src, recursive);
    }

    public boolean rename(String src, String dst) throws IOException {
        checkOpen();
        return namenode.rename(src, dst);
    }

    public HdfsFileStatus getFileInfo(String src) throws IOException {
        checkOpen();
        return namenode.getFileInfo(src);
    }

    public HdfsFileStatus[] listPaths(String src) throws IOException {
        checkOpen();
        return namenode.getListing(src);
    }

    public boolean recoverLease(String src) throws IOException {
        checkOpen();
        return namenode.recoverLease(src, clientName);
    }

    public String metaSave() throws IOException {
        return namenode.metaSave();
    }

    /**
     * EduDFS: for every block of the file and every replica, the local block file on the DataNode
     * (asks the NameNode for the locations, then each DataNode via ClientDatanodeProtocol).
     * A DataNode that cannot be reached yields an entry with a null path.
     */
    public List<BlockLocalPathInfo> getBlockLocalPaths(String src) throws IOException {
        checkOpen();
        List<BlockLocalPathInfo> res = new ArrayList<>();
        for (LocatedBlock lb : namenode.getBlockLocations(src, 0, Long.MAX_VALUE).getLocatedBlocks()) {
            for (DatanodeInfo dn : lb.getLocations()) {
                try {
                    ClientDatanodeProtocol cdp = (ClientDatanodeProtocol) LocateRegistry
                            .getRegistry(dn.getHostName(), dn.getIpcPort()).lookup(ClientDatanodeProtocol.NAME);
                    res.add(cdp.getBlockLocalPathInfo(lb.getBlock()));
                } catch (IOException | NotBoundException e) {
                    res.add(new BlockLocalPathInfo(lb.getBlock(), null, null));
                }
            }
        }
        return res;
    }

    public DatanodeInfo[] datanodeReport() throws IOException {
        return namenode.getDatanodeReport();
    }

    void reportBadBlock(LocatedBlock lb, DatanodeInfo dn) {
        try {
            namenode.reportBadBlocks(new LocatedBlock[]{
                    new LocatedBlock(lb.getBlock(), lb.getStartOffset(), new DatanodeInfo[]{dn})});
        } catch (IOException ignored) {
            // just a hint to the NameNode
        }
    }

    // ------------------------------------------------------------- Leases
    synchronized void beginFileLease(long fileId, DFSOutputStream out) {
        filesBeingWritten.put(fileId, out);
        leaseRenewer.start();
    }

    synchronized void endFileLease(long fileId) {
        filesBeingWritten.remove(fileId);
    }

    synchronized boolean isFilesBeingWrittenEmpty() {
        return filesBeingWritten.isEmpty();
    }

    /** Called by the LeaseRenewer. */
    void renewLease() throws IOException {
        if (clientRunning && !isFilesBeingWrittenEmpty()) {
            namenode.renewLease(clientName);
        }
    }

    // ------------------------------------------------------------ Closing
    /** Normal close: cleanly complete all open files. */
    @Override
    public void close() throws IOException {
        List<DFSOutputStream> open;
        synchronized (this) {
            open = new ArrayList<>(filesBeingWritten.values());
        }
        for (DFSOutputStream out : open) {
            out.close();
        }
        clientRunning = false;
        leaseRenewer.stop();
    }

    /**
     * Simulates a client crash: no more lease renewal, connections are cut
     * abruptly, complete() is never called. The files stay "under construction"
     * until the lease has expired and the NameNode closes them via lease recovery.
     */
    public void abort() {
        clientRunning = false;
        leaseRenewer.stop();
        List<DFSOutputStream> open;
        synchronized (this) {
            open = new ArrayList<>(filesBeingWritten.values());
            filesBeingWritten.clear();
        }
        for (DFSOutputStream out : open) {
            out.abort();
        }
    }
}
