package edudfs.server.namenode;

import edudfs.protocol.Block;
import edudfs.protocol.ClientProtocol;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LastBlockWithStatus;
import edudfs.protocol.LocatedBlock;
import edudfs.protocol.LocatedBlocks;
import edudfs.server.protocol.DatanodeCommand;
import edudfs.server.protocol.DatanodeProtocol;
import edudfs.server.protocol.StorageBlockReport;
import edudfs.server.protocol.StorageReceivedDeletedBlocks;
import edudfs.server.protocol.StorageReport;

import java.io.IOException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer:
 * implements the RPC interfaces and delegates to FSNamesystem.
 */
public class NameNodeRpcServer implements ClientProtocol, DatanodeProtocol {

    private final FSNamesystem namesystem;

    NameNodeRpcServer(FSNamesystem namesystem) {
        this.namesystem = namesystem;
    }

    private static String clientMachine() {
        try {
            return RemoteServer.getClientHost();
        } catch (ServerNotActiveException e) {
            return "local";
        }
    }

    // ------------------------------------------------------ ClientProtocol
    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
        return namesystem.getBlockLocations(src, offset, length);
    }

    @Override
    public HdfsFileStatus create(String src, String clientName, boolean overwrite, short replication, long blockSize)
            throws IOException {
        return namesystem.startFile(src, clientName, clientMachine(), overwrite, replication, blockSize);
    }

    @Override
    public LastBlockWithStatus append(String src, String clientName) throws IOException {
        return namesystem.appendFile(src, clientName, clientMachine());
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, Block previous, long fileId,
                                 DatanodeInfo[] excludeNodes) throws IOException {
        return namesystem.getAdditionalBlock(src, fileId, clientName, previous, excludeNodes);
    }

    @Override
    public void abandonBlock(Block b, long fileId, String src, String holder) throws IOException {
        namesystem.abandonBlock(b, fileId, src, holder);
    }

    @Override
    public boolean complete(String src, String clientName, Block last, long fileId) throws IOException {
        return namesystem.completeFile(src, clientName, last, fileId);
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] blocks) {
        namesystem.reportBadBlocks(blocks);
    }

    @Override
    public void renewLease(String clientName) {
        namesystem.renewLease(clientName);
    }

    @Override
    public boolean recoverLease(String src, String clientName) throws IOException {
        return namesystem.recoverLease(src, clientName);
    }

    @Override
    public boolean mkdirs(String src) throws IOException {
        return namesystem.mkdirs(src);
    }

    @Override
    public boolean delete(String src, boolean recursive) throws IOException {
        return namesystem.delete(src, recursive);
    }

    @Override
    public boolean rename(String src, String dst) throws IOException {
        return namesystem.rename(src, dst);
    }

    @Override
    public HdfsFileStatus getFileInfo(String src) throws IOException {
        return namesystem.getFileInfo(src);
    }

    @Override
    public HdfsFileStatus[] getListing(String src) throws IOException {
        return namesystem.getListing(src);
    }

    @Override
    public DatanodeInfo[] getDatanodeReport() {
        namesystem.readLock();
        try {
            return namesystem.getBlockManager().getDatanodeManager().getDatanodeReport();
        } finally {
            namesystem.readUnlock();
        }
    }

    @Override
    public String metaSave() {
        return namesystem.metaSave();
    }

    // ---------------------------------------------------- DatanodeProtocol
    @Override
    public DatanodeID registerDatanode(DatanodeID registration) {
        return namesystem.registerDatanode(registration);
    }

    @Override
    public DatanodeCommand[] sendHeartbeat(DatanodeID registration, StorageReport[] reports, int failedVolumes) {
        return namesystem.handleHeartbeat(registration, reports, failedVolumes);
    }

    @Override
    public DatanodeCommand blockReport(DatanodeID registration, String poolId, StorageBlockReport[] reports)
            throws IOException {
        return namesystem.processReport(registration, poolId, reports);
    }

    @Override
    public void blockReceivedAndDeleted(DatanodeID registration, String poolId, StorageReceivedDeletedBlocks[] blocks)
            throws IOException {
        namesystem.processIncrementalBlockReport(registration, poolId, blocks);
    }

    @Override
    public void commitBlockSynchronization(Block block, long newGenerationStamp, long newLength, boolean closeFile,
                                           boolean deleteBlock, DatanodeID[] newTargets, String[] newTargetStorages)
            throws IOException {
        namesystem.commitBlockSynchronization(block, newGenerationStamp, newLength, closeFile, deleteBlock,
                newTargets, newTargetStorages);
    }
}
