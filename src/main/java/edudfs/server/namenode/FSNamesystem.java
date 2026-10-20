package edudfs.server.namenode;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.common.Time;
import edudfs.protocol.AlreadyBeingCreatedException;
import edudfs.protocol.Block;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.FileAlreadyExistsException;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LastBlockWithStatus;
import edudfs.protocol.LeaseExpiredException;
import edudfs.protocol.LocatedBlock;
import edudfs.protocol.LocatedBlocks;
import edudfs.protocol.RecoveryInProgressException;
import edudfs.server.blockmanagement.BlockIdManager;
import edudfs.server.blockmanagement.BlockInfo;
import edudfs.server.blockmanagement.BlockManager;
import edudfs.server.blockmanagement.BlockUnderConstructionFeature;
import edudfs.server.blockmanagement.DatanodeDescriptor;
import edudfs.server.blockmanagement.DatanodeStorageInfo;
import edudfs.server.blockmanagement.DatanodeManager;
import edudfs.server.common.HdfsServerConstants;
import edudfs.server.common.HdfsServerConstants.BlockUCState;
import edudfs.server.namenode.LeaseManager.Lease;
import edudfs.server.protocol.DatanodeStorage;
import edudfs.server.protocol.StorageBlockReport;
import edudfs.server.protocol.StorageReceivedDeletedBlocks;
import edudfs.server.protocol.StorageReport;
import edudfs.server.protocol.DatanodeCommand;
import edudfs.server.protocol.DatanodeProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.FSNamesystem.
 *
 * Facade over the namespace (FSDirectory), blocks (BlockManager) and leases (LeaseManager).
 * A global ReadWriteLock protects all in-memory structures - as in the original.
 */
public class FSNamesystem {

    private static final String LOG = "FSNamesystem";

    enum RecoverLeaseOp {
        CREATE_FILE,
        APPEND_FILE,
        RECOVER_LEASE
    }

    private final ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
    private final FSDirectory dir;
    private final BlockManager blockManager;
    private final BlockIdManager blockIdManager;
    private final LeaseManager leaseManager;

    public FSNamesystem() {
        dir = new FSDirectory();
        blockManager = new BlockManager(this);
        blockIdManager = new BlockIdManager(blockManager);
        leaseManager = new LeaseManager(this);
    }

    void startActiveServices() {
        blockManager.activate();
        leaseManager.startMonitor();
    }

    void stopActiveServices() {
        leaseManager.stopMonitor();
        blockManager.close();
    }

    // --------------------------------------------------------------- Lock
    public void readLock() {
        fsLock.readLock().lock();
    }

    public void readUnlock() {
        fsLock.readLock().unlock();
    }

    public void writeLock() {
        fsLock.writeLock().lock();
    }

    public void writeUnlock() {
        fsLock.writeLock().unlock();
    }

    public BlockManager getBlockManager() {
        return blockManager;
    }

    public LeaseManager getLeaseManager() {
        return leaseManager;
    }

    public FSDirectory getFSDirectory() {
        return dir;
    }

    /** BlockInfo.bcId -> file. */
    public INodeFile getBlockCollection(long inodeId) {
        INode n = dir.getInode(inodeId);
        return n != null && n.isFile() ? n.asFile() : null;
    }

    // =====================================================================
    //                         ClientProtocol
    // =====================================================================

    HdfsFileStatus startFile(String src, String holder, String clientMachine, boolean overwrite,
                             short replication, long blockSize) throws IOException {
        if (blockSize <= 0 || blockSize % HdfsConfig.BYTES_PER_CHECKSUM != 0) {
            throw new IOException("blockSize must be a positive multiple of " + HdfsConfig.BYTES_PER_CHECKSUM);
        }
        if (replication < HdfsConfig.MIN_REPLICATION) {
            throw new IOException("Requested replication " + replication + " is less than the required minimum "
                    + HdfsConfig.MIN_REPLICATION);
        }
        writeLock();
        try {
            INodesInPath iip = dir.getINodesInPath(src);
            if (iip.length() == 1) {
                throw new IOException("Cannot create file: " + src + " is the root directory");
            }
            INode existing = iip.getLastINode();
            if (existing != null) {
                if (existing.isDirectory()) {
                    throw new FileAlreadyExistsException(src + " already exists as a directory");
                }
                if (overwrite) {
                    deleteInternal(existing);  // also removes the old writer's lease, if any
                } else {
                    // If the file is open and the soft limit has expired, recovery is triggered here
                    recoverLeaseInternal(RecoverLeaseOp.CREATE_FILE, existing.asFile(), src, holder, false);
                    throw new FileAlreadyExistsException(src + " for client " + holder + " already exists");
                }
            }
            long now = Time.now();
            INodeDirectory parent = dir.mkdirs(iip.getParentPath(), now);
            INodeFile file = dir.addFile(parent, iip.getLastLocalName(), replication, blockSize, holder,
                    clientMachine, now);
            leaseManager.addLease(holder, file.getId());
            Log.info(LOG, "DIR* startFile: " + src + " (inode " + file.getId() + ") lease granted to " + holder);
            return dir.createFileStatus(file);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Corresponds to FSDirAppendOp.appendFile with CreateFlag.NEW_BLOCK: the file becomes
     * "under construction" again, gets a lease, and the next addBlock() appends a new
     * block. The previous last block stays COMPLETE and unchanged (no continued writing
     * into a half-full block - that would require a new GS and a pipeline on
     * existing replicas).
     */
    LastBlockWithStatus appendFile(String src, String holder, String clientMachine) throws IOException {
        writeLock();
        try {
            INode inode = dir.getINodesInPath(src).getLastINode();
            if (inode == null) {
                throw new FileNotFoundException("Failed to append to non-existent file " + src + " for client "
                        + holder);
            }
            if (inode.isDirectory()) {
                throw new FileNotFoundException("Failed to append to " + src + ": it is a directory");
            }
            INodeFile file = inode.asFile();
            // Open file: as with create() - AlreadyBeingCreated or recovery after the soft limit
            recoverLeaseInternal(RecoverLeaseOp.APPEND_FILE, file, src, holder, false);
            if (file.isUnderConstruction()) {
                throw new RecoveryInProgressException("Failed to append to " + src + ", lease recovery is in progress."
                        + " Try again later.");
            }
            BlockInfo last = file.getLastBlock();
            LocatedBlock lastBlock = last == null ? null
                    : new LocatedBlock(new Block(last), file.computeFileSize(true) - last.getNumBytes(),
                    BlockManager.toDatanodeInfos(last.getDatanodes()));
            file.toUnderConstruction(holder, clientMachine);
            leaseManager.addLease(holder, file.getId());
            Log.info(LOG, "DIR* appendFile: " + src + " (inode " + file.getId() + ", last block " + last
                    + ") lease granted to " + holder);
            return new LastBlockWithStatus(lastBlock, dir.createFileStatus(file));
        } finally {
            writeUnlock();
        }
    }

    LocatedBlock getAdditionalBlock(String src, long fileId, String clientName, Block previous,
                                    DatanodeInfo[] excludedNodes) throws IOException {
        writeLock();
        try {
            INodeFile file = getFileForWrite(src, fileId);
            checkLease(src, clientName, file);

            // 1) commit the previous block (the client reports its final length)
            if (previous != null) {
                BlockInfo last = file.getLastBlock();
                if (last == null || !last.equals(previous)) {
                    throw new IOException("addBlock: " + previous + " is not the last block of " + src);
                }
                blockManager.commitOrCompleteLastBlock(file, previous);
            } else if (file.numBlocks() > 0) {
                throw new IOException("addBlock: previous block must be given for " + src);
            }

            // 2) choose pipeline targets
            Set<DatanodeID> excluded = excludedNodes == null ? Set.of() : new HashSet<>(Arrays.asList(excludedNodes));
            short replication = file.getFileReplication();
            DatanodeStorageInfo[] targets = blockManager.chooseTarget4NewBlock(replication, excluded,
                    file.getPreferredBlockSize());
            if (targets.length < HdfsConfig.MIN_REPLICATION) {
                throw new IOException("File " + src + " could only be written to " + targets.length + " of the "
                        + HdfsConfig.MIN_REPLICATION + " minReplication nodes. There are "
                        + blockManager.getDatanodeManager().getLiveDatanodes().size()
                        + " datanode(s) running and " + excluded.size() + " node(s) are excluded in this operation.");
            }

            // 3) create a new block: sequential block ID + new generation stamp
            long offset = file.computeFileSize(true);
            Block newBlock = new Block(blockIdManager.nextBlockId(), 0, blockIdManager.nextGenerationStamp());
            BlockInfo blockInfo = new BlockInfo(newBlock, replication);
            blockInfo.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION, targets);
            blockManager.addBlockCollection(blockInfo, file.getId());
            file.addBlock(blockInfo);
            Log.info(LOG, "BLOCK* allocate " + newBlock + " for " + src + ", pipeline " + Arrays.toString(targets));
            return new LocatedBlock(new Block(newBlock), offset,
                    BlockManager.toDatanodeInfos(blockInfo.getUnderConstructionFeature().getExpectedLocations()));
        } finally {
            writeUnlock();
        }
    }

    void abandonBlock(Block b, long fileId, String src, String holder) throws IOException {
        writeLock();
        try {
            INodeFile file = getFileForWrite(src, fileId);
            checkLease(src, holder, file);
            BlockInfo last = file.getLastBlock();
            if (last != null && last.equals(b) && !last.isComplete()) {
                file.removeLastBlock(last);
                blockManager.removeBlock(last);
                Log.info(LOG, "BLOCK* abandonBlock: " + b + " of file " + src);
            }
        } finally {
            writeUnlock();
        }
    }

    boolean completeFile(String src, String holder, Block last, long fileId) throws IOException {
        writeLock();
        try {
            INodeFile file = getFileForWrite(src, fileId);
            if (!file.isUnderConstruction()) {
                // Retry of an already successful complete()?
                BlockInfo stored = file.getLastBlock();
                if (last == null ? stored == null : last.equals(stored)) {
                    return true;
                }
            }
            checkLease(src, holder, file);
            blockManager.commitOrCompleteLastBlock(file, last);
            for (BlockInfo b : file.getBlocks()) {
                if (!b.isComplete()) {
                    Log.info(LOG, "DIR* completeFile: " + src + " not yet closed - " + b + " is "
                            + b.getBlockUCState() + " (waiting for DataNode reports)");
                    return false;
                }
            }
            finalizeINodeFileUnderConstruction(src, file);
            return true;
        } finally {
            writeUnlock();
        }
    }

    LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
        readLock();
        try {
            INode inode = dir.getINodesInPath(src).getLastINode();
            if (inode == null || !inode.isFile()) {
                throw new FileNotFoundException("File does not exist: " + src);
            }
            INodeFile file = inode.asFile();
            List<LocatedBlock> result = new ArrayList<>();
            long pos = 0;
            for (BlockInfo b : file.getBlocks()) {
                long len = b.getNumBytes();
                if (pos + len > offset && pos < offset + length) {
                    List<DatanodeDescriptor> locs = new ArrayList<>();
                    for (DatanodeStorageInfo s : b.getStorageInfos()) {
                        if (s.getState() == DatanodeStorage.State.NORMAL && s.getDatanodeDescriptor().isAlive()) {
                            locs.add(s.getDatanodeDescriptor());
                        }
                    }
                    if (locs.isEmpty() && b.getUnderConstructionFeature() != null) {
                        locs.addAll(b.getUnderConstructionFeature().getExpectedLocations());
                    }
                    result.add(new LocatedBlock(new Block(b), pos, BlockManager.toDatanodeInfos(locs)));
                }
                pos += len;
            }
            // Simplification: readers only see the completed blocks of an open file
            // (original: the client asks the DataNodes for the "visible length" of the last block).
            return new LocatedBlocks(file.computeFileSize(false), file.isUnderConstruction(), result);
        } finally {
            readUnlock();
        }
    }

    void reportBadBlocks(LocatedBlock[] blocks) {
        writeLock();
        try {
            for (LocatedBlock lb : blocks) {
                for (DatanodeInfo dn : lb.getLocations()) {
                    blockManager.markBlockAsCorrupt(lb.getBlock(), dn);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    boolean mkdirs(String src) throws IOException {
        writeLock();
        try {
            INode existing = dir.getINodesInPath(src).getLastINode();
            if (existing != null && !existing.isDirectory()) {
                throw new FileAlreadyExistsException("Path is not a directory: " + src);
            }
            dir.mkdirs(src, Time.now());
            return true;
        } finally {
            writeUnlock();
        }
    }

    boolean delete(String src, boolean recursive) throws IOException {
        writeLock();
        try {
            INode target = dir.getINodesInPath(src).getLastINode();
            if (target == null) {
                return false;
            }
            if (target.isRoot()) {
                throw new IOException("Cannot delete root directory");
            }
            if (target.isDirectory() && !recursive && !target.asDirectory().getChildrenList().isEmpty()) {
                throw new IOException(src + " is non empty");
            }
            deleteInternal(target);
            return true;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Deletes a subtree. The blocks are NOT deleted on the DataNodes immediately:
     * they go into the invalidate queues and are distributed via heartbeat responses.
     */
    private void deleteInternal(INode target) {
        String path = target.getFullPathName();
        List<BlockInfo> blocks = new ArrayList<>();
        List<INode> removed = new ArrayList<>();
        dir.delete(target, blocks, removed, Time.now());
        for (INode n : removed) {
            if (n.isFile() && n.asFile().isUnderConstruction()) {
                leaseManager.removeLease(n.getId());
            }
        }
        for (BlockInfo b : blocks) {
            blockManager.removeBlock(b);
        }
        Log.info(LOG, "DIR* delete: " + path + " (" + removed.size() + " inode(s), " + blocks.size() + " block(s))");
    }

    boolean rename(String src, String dst) throws IOException {
        writeLock();
        try {
            boolean ok = dir.rename(src, dst, Time.now());
            if (ok) {
                Log.info(LOG, "DIR* rename: " + src + " -> " + dst);
            }
            return ok;
        } finally {
            writeUnlock();
        }
    }

    HdfsFileStatus getFileInfo(String src) throws IOException {
        readLock();
        try {
            INode node = dir.getINodesInPath(src).getLastINode();
            return node == null ? null : dir.createFileStatus(node);
        } finally {
            readUnlock();
        }
    }

    HdfsFileStatus[] getListing(String src) throws IOException {
        readLock();
        try {
            INode node = dir.getINodesInPath(src).getLastINode();
            if (node == null) {
                throw new FileNotFoundException("File " + src + " does not exist.");
            }
            if (node.isFile()) {
                return new HdfsFileStatus[]{dir.createFileStatus(node)};
            }
            List<HdfsFileStatus> r = new ArrayList<>();
            for (INode child : node.asDirectory().getChildrenList()) {
                r.add(dir.createFileStatus(child));
            }
            return r.toArray(new HdfsFileStatus[0]);
        } finally {
            readUnlock();
        }
    }

    void renewLease(String holder) {
        // As in the original: the LeaseManager is synchronized internally
        leaseManager.renewLease(holder);
    }

    /** Forced lease recovery. Returns true if the file is (now) closed. */
    boolean recoverLease(String src, String holder) throws IOException {
        writeLock();
        try {
            INode inode = dir.getINodesInPath(src).getLastINode();
            if (inode == null || !inode.isFile()) {
                throw new FileNotFoundException("File not found " + src);
            }
            INodeFile file = inode.asFile();
            if (!file.isUnderConstruction()) {
                return true;
            }
            recoverLeaseInternal(RecoverLeaseOp.RECOVER_LEASE, file, src, holder, true);
            return !file.isUnderConstruction();
        } finally {
            writeUnlock();
        }
    }

    // =====================================================================
    //                         Lease logic
    // =====================================================================

    private INodeFile getFileForWrite(String src, long fileId) throws IOException {
        // Preferably by INode ID: robust against renames during writing
        INode inode = fileId != INodeId.GRANDFATHER_INODE_ID
                ? dir.getInode(fileId) : dir.getINodesInPath(src).getLastINode();
        if (inode == null || !inode.isFile()) {
            throw new FileNotFoundException("File does not exist: " + src + " (inode " + fileId + ")");
        }
        return inode.asFile();
    }

    /** May 'holder' write to this file? */
    private void checkLease(String src, String holder, INodeFile file) throws LeaseExpiredException {
        if (!file.isUnderConstruction()) {
            throw new LeaseExpiredException("No lease on " + src + ": File is not open for writing. Holder "
                    + holder + " does not have any open files.");
        }
        String clientName = file.getFileUnderConstructionFeature().getClientName();
        if (!clientName.equals(holder)) {
            throw new LeaseExpiredException("No lease on " + src + ": Client (=" + holder
                    + ") is not the lease owner (=" + clientName + ")");
        }
    }

    private void recoverLeaseInternal(RecoverLeaseOp op, INodeFile file, String src, String holder, boolean force)
            throws IOException {
        if (!file.isUnderConstruction()) {
            return;
        }
        Lease lease = leaseManager.getLease(file);
        String clientName = file.getFileUnderConstructionFeature().getClientName();
        if (!force && clientName.equals(holder)) {
            throw new AlreadyBeingCreatedException("Failed to " + op + " " + src + " for " + holder
                    + " because current leaseholder is trying to recreate file.");
        }
        if (force) {
            Log.info(LOG, "recoverLease: " + lease + ", src=" + src + " from client " + holder);
            internalReleaseLease(lease, src, file, HdfsServerConstants.NAMENODE_LEASE_HOLDER);
            return;
        }
        long now = Time.monotonicNow();
        if (lease != null && lease.expiredSoftLimit(now)) {
            Log.info(LOG, "startFile: soft limit of " + lease + " expired -> recover lease for " + src);
            if (internalReleaseLease(lease, src, file, null)) {
                return;  // could be closed immediately
            }
            throw new RecoveryInProgressException("Failed to " + op + " " + src + " for " + holder
                    + ", lease recovery is in progress. Try again later.");
        }
        BlockInfo last = file.getLastBlock();
        if (last != null && last.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
            throw new RecoveryInProgressException("Recovery in progress, file [" + src + "], lease owner ["
                    + clientName + "]");
        }
        throw new AlreadyBeingCreatedException("Failed to " + op + " " + src + " for " + holder
                + " because this file is already being created by " + clientName);
    }

    /**
     * Core of lease recovery (FSNamesystem.internalReleaseLease).
     *
     * @return true if the file could be closed immediately; false if a
     *         block recovery was triggered on the DataNodes (asynchronous).
     */
    boolean internalReleaseLease(Lease lease, String src, INodeFile file, String recoveryLeaseHolder)
            throws IOException {
        BlockInfo last = file.getLastBlock();
        BlockInfo penultimate = file.getPenultimateBlock();

        if (last == null) {
            finalizeINodeFileUnderConstruction(src, file);  // empty file
            return true;
        }
        if (penultimate != null && !penultimate.isComplete()) {
            if (penultimate.getBlockUCState() == BlockUCState.COMMITTED && blockManager.hasMinStorage(penultimate)) {
                blockManager.completeBlock(penultimate);
            } else if (penultimate.getBlockUCState() == BlockUCState.COMMITTED) {
                Log.info(LOG, "internalReleaseLease: penultimate block " + penultimate + " of " + src
                        + " is waiting to be minimally replicated");
                return false;
            } else {
                throw new IOException("DIR* NameSystem.internalReleaseLease: penultimate block " + penultimate
                        + " of " + src + " is " + penultimate.getBlockUCState() + " and not minimally replicated");
            }
        }

        switch (last.getBlockUCState()) {
            case COMPLETE:
                finalizeINodeFileUnderConstruction(src, file);
                return true;

            case COMMITTED:
                if (blockManager.hasMinStorage(last)) {
                    blockManager.completeBlock(last);
                    finalizeINodeFileUnderConstruction(src, file);
                    return true;
                }
                Log.info(LOG, "internalReleaseLease: committed block " + last + " of " + src
                        + " is waiting to be minimally replicated");
                return false;

            case UNDER_CONSTRUCTION:
            case UNDER_RECOVERY:
                BlockUnderConstructionFeature uc = last.getUnderConstructionFeature();
                if (uc.getNumExpectedLocations() == 0 && last.getNumBytes() == 0) {
                    // Block was allocated but never written -> simply remove it
                    file.removeLastBlock(last);
                    blockManager.removeBlock(last);
                    finalizeINodeFileUnderConstruction(src, file);
                    return true;
                }
                if (last.getBlockUCState() == BlockUCState.UNDER_RECOVERY
                        && Time.monotonicNow() - uc.getRecoveryStartTime() < HdfsConfig.BLOCK_RECOVERY_TIMEOUT_MS) {
                    Log.info(LOG, "internalReleaseLease: recovery of " + last + " already in progress");
                    return false;
                }
                // New GS = recoveryId: replicas with the old GS are then recognizably stale
                long recoveryId = blockIdManager.nextGenerationStamp();
                String newHolder = recoveryLeaseHolder != null
                        ? recoveryLeaseHolder : HdfsServerConstants.NAMENODE_LEASE_HOLDER;
                leaseManager.reassignLease(lease, file, newHolder);
                DatanodeDescriptor primary = blockManager.initializeBlockRecovery(last, recoveryId);
                if (primary == null) {
                    Log.warn(LOG, "BLOCK* internalReleaseLease: no live DataNode for " + last
                            + " - recovery will be retried");
                } else {
                    Log.info(LOG, "BLOCK* internalReleaseLease: lease of " + src + " reassigned to " + newHolder
                            + "; recovery of " + last + " started, recoveryId=" + recoveryId + ", primary=" + primary);
                }
                return false;

            default:
                throw new IllegalStateException("Unexpected state " + last.getBlockUCState());
        }
    }

    /** Close the file: remove the lease, remove the UC feature, check replication. */
    private void finalizeINodeFileUnderConstruction(String src, INodeFile file) {
        String holder = file.getFileUnderConstructionFeature().getClientName();
        leaseManager.removeLease(file.getId());
        file.toCompleteFile(Time.now());
        for (BlockInfo b : file.getBlocks()) {
            blockManager.checkRedundancy(b);
        }
        Log.info(LOG, "DIR* completeFile: " + src + " is closed by " + holder + " (length "
                + file.computeFileSize(true) + ")");
    }

    // =====================================================================
    //                         DatanodeProtocol
    // =====================================================================

    DatanodeID registerDatanode(DatanodeID reg) {
        writeLock();
        try {
            blockManager.getDatanodeManager().registerDatanode(reg);
            return reg;
        } finally {
            writeUnlock();
        }
    }

    DatanodeCommand[] handleHeartbeat(DatanodeID reg, StorageReport[] reports, int failedVolumes) {
        writeLock();
        try {
            return blockManager.getDatanodeManager().handleHeartbeat(reg, reports, failedVolumes);
        } finally {
            writeUnlock();
        }
    }

    /** As in the original: reports for a foreign block pool are rejected. */
    private static void checkBlockPoolId(String poolId) throws IOException {
        if (!HdfsServerConstants.BLOCK_POOL_ID.equals(poolId)) {
            throw new IOException("Unknown block pool " + poolId + " (this namenode serves "
                    + HdfsServerConstants.BLOCK_POOL_ID + ")");
        }
    }

    DatanodeCommand processReport(DatanodeID reg, String poolId, StorageBlockReport[] report) throws IOException {
        checkBlockPoolId(poolId);
        writeLock();
        try {
            DatanodeDescriptor dn = blockManager.getDatanodeManager().getDatanode(reg);
            if (dn == null || !dn.isAlive()) {
                return new DatanodeCommand(DatanodeProtocol.DNA_REGISTER);
            }
            blockManager.processReport(dn, report);
            return null;
        } finally {
            writeUnlock();
        }
    }

    void processIncrementalBlockReport(DatanodeID reg, String poolId, StorageReceivedDeletedBlocks[] infos)
            throws IOException {
        checkBlockPoolId(poolId);
        writeLock();
        try {
            DatanodeDescriptor dn = blockManager.getDatanodeManager().getDatanode(reg);
            if (dn == null || !dn.isAlive()) {
                throw new IOException("Unregistered datanode " + reg);
            }
            blockManager.processIncrementalBlockReport(dn, infos);
        } finally {
            writeUnlock();
        }
    }

    /** The primary DataNode reports the result of the block recovery. */
    void commitBlockSynchronization(Block oldBlock, long newGs, long newLength, boolean closeFile,
                                    boolean deleteBlock, DatanodeID[] newTargets, String[] newTargetStorages)
            throws IOException {
        writeLock();
        try {
            BlockInfo stored = blockManager.getStoredBlock(oldBlock);
            if (stored == null || stored.isDeleted()) {
                if (deleteBlock) {
                    return;
                }
                throw new IOException("Block (=" + oldBlock + ") not found");
            }
            INodeFile file = getBlockCollection(stored.getBlockCollectionId());
            if (file == null || !file.isUnderConstruction()) {
                Log.info(LOG, "commitBlockSynchronization: file of " + oldBlock + " is already closed");
                return;
            }
            BlockUnderConstructionFeature uc = stored.getUnderConstructionFeature();
            if (uc == null || uc.getBlockUCState() != BlockUCState.UNDER_RECOVERY) {
                throw new IOException("Block " + stored + " is not under recovery");
            }
            if (uc.getBlockRecoveryId() != newGs) {
                throw new IOException("The recovery id " + newGs + " does not match current recovery id "
                        + uc.getBlockRecoveryId() + " for block " + oldBlock);
            }
            String src = file.getFullPathName();
            if (deleteBlock) {
                file.removeLastBlock(stored);
                blockManager.removeBlock(stored);
            } else {
                blockManager.commitBlockSynchronization(stored, newGs, newLength, newTargets, newTargetStorages,
                        closeFile);
            }
            Log.info(LOG, "commitBlockSynchronization(oldBlock=" + oldBlock + ", newgs=" + newGs + ", newlength="
                    + newLength + ", newtargets=" + Arrays.toString(newTargets) + ", closeFile=" + closeFile
                    + ", deleteBlock=" + deleteBlock + ") successful");
            if (closeFile) {
                finalizeINodeFileUnderConstruction(src, file);
            }
        } finally {
            writeUnlock();
        }
    }

    // =====================================================================
    //                         MetaSave (teaching dump)
    // =====================================================================

    String metaSave() {
        readLock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("==================== NAMESPACE (INode tree) ====================\n");
            sb.append("INodes: ").append(dir.getInodeCount()).append(", last INode ID: ")
                    .append(dir.getLastInodeId()).append(", Blocks in BlocksMap: ")
                    .append(blockManager.getTotalBlocks()).append(", last generation stamp: ")
                    .append(blockIdManager.getGenerationStamp()).append('\n');
            dumpTree(dir.getRoot(), "", sb);

            long now = Time.monotonicNow();
            sb.append("==================== INODES (objects in the INodeMap, by ID) ====================\n");
            for (INode n : dir.getInodesSortedById()) {
                dumpINode(n, now, sb);
            }

            sb.append("==================== LEASES ====================\n");
            List<Lease> leases = leaseManager.getLeases();
            if (leases.isEmpty()) {
                sb.append("(none)\n");
            }
            for (Lease l : leases) {
                sb.append(String.format("holder=%s  last renewed %.1f s ago%s%s  files=",
                        l.getHolder(), (now - l.getLastUpdate()) / 1000.0,
                        l.expiredSoftLimit(now) ? "  [SOFT LIMIT expired]" : "",
                        l.expiredHardLimit(now) ? "  [HARD LIMIT expired]" : ""));
                List<String> files = new ArrayList<>();
                for (Long id : l.getFiles()) {
                    INode n = dir.getInode(id);
                    files.add(id + ":" + (n == null ? "?" : n.getFullPathName()));
                }
                sb.append(files).append('\n');
            }

            sb.append("==================== DATANODES ====================\n");
            DatanodeManager dm = blockManager.getDatanodeManager();
            for (DatanodeDescriptor dn : dm.getDatanodes()) {
                sb.append(String.format("%s  %s  blocks=%d  last heartbeat %.1f s ago  uuid=%s%s%s%n",
                        dn.getXferAddr(), dn.isAlive() ? "LIVE" : "DEAD", dn.getNumBlocks(),
                        (now - dn.getLastUpdateMonotonic()) / 1000.0, dn.getDatanodeUuid(),
                        dn.getVolumeFailures() == 0 ? "" : "  failed volumes=" + dn.getVolumeFailures(),
                        dn.getPendingInvalidations().isEmpty() ? ""
                                : "  to delete=" + dn.getPendingInvalidations()));
                for (DatanodeStorageInfo s : dn.getStorageInfos()) {
                    sb.append(String.format("    storage %s  %s  %s  blocks=%d  used=%d B  free=%d MB%n",
                            s.getStorageID(), s.getStorageType(), s.getState(), s.numBlocks(), s.getDfsUsed(),
                            s.getRemaining() >> 20));
                }
            }
            sb.append("==================== REPLICATION ====================\n");
            sb.append("neededReconstruction: ").append(blockManager.getNeededReconstruction()).append('\n');
            sb.append("pendingReconstruction: ").append(blockManager.getPendingReconstruction()).append('\n');
            return sb.toString();
        } finally {
            readUnlock();
        }
    }

    /**
     * One INode object with its fields. The full path is NOT a field but is
     * computed via parent; the lease is not attached to the INode but lives in the
     * LeaseManager (key: INode ID) - the INode only holds the holder in the UC feature.
     */
    private void dumpINode(INode n, long now, StringBuilder sb) {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sb.append(n.getClass().getSimpleName()).append(" id=").append(n.getId()).append('\n');
        sb.append("    localName=\"").append(n.getLocalName()).append('"')
                .append("  parent=").append(n.getParent() == null ? "null" : n.getParent().getId())
                .append("  mtime=").append(fmt.format(new Date(n.getModificationTime())))
                .append("   (computed path: ").append(n.getFullPathName()).append(")\n");
        if (n.isDirectory()) {
            List<String> children = new ArrayList<>();
            for (INode c : n.asDirectory().getChildrenList()) {
                children.add(c.getId() + ":" + c.getLocalName());
            }
            sb.append("    children=").append(children.isEmpty() ? "null" : children).append('\n');
            return;
        }
        INodeFile f = n.asFile();
        sb.append("    header: repl=").append(f.getFileReplication())
                .append("  preferredBlockSize=").append(f.getPreferredBlockSize()).append('\n');
        List<String> blocks = new ArrayList<>();
        for (BlockInfo b : f.getBlocks()) {
            blocks.add(b + "(" + b.getBlockUCState() + ")");
        }
        sb.append("    blocks=").append(blocks).append('\n');
        FileUnderConstructionFeature uc = f.getFileUnderConstructionFeature();
        sb.append("    ucFeature=").append(uc == null ? "null"
                : "{clientName=" + uc.getClientName() + ", clientMachine=" + uc.getClientMachine() + "}").append('\n');
        Lease l = leaseManager.getLease(f);
        sb.append("    lease (LeaseManager.leasesById[").append(f.getId()).append("])=");
        if (l == null) {
            sb.append("null\n");
        } else {
            sb.append(String.format("{holder=%s, renewed %.1f s ago%s%s}%n", l.getHolder(),
                    (now - l.getLastUpdate()) / 1000.0,
                    l.expiredSoftLimit(now) ? ", SOFT LIMIT expired" : "",
                    l.expiredHardLimit(now) ? ", HARD LIMIT expired" : ""));
        }
    }

    private void dumpTree(INode node, String indent, StringBuilder sb) {
        if (node.isDirectory()) {
            sb.append(indent).append(node.isRoot() ? "/" : node.getLocalName() + "/")
                    .append("   [dir, inode=").append(node.getId()).append("]\n");
            for (INode child : node.asDirectory().getChildrenList()) {
                dumpTree(child, indent + "    ", sb);
            }
            return;
        }
        INodeFile f = node.asFile();
        sb.append(indent).append(f.getLocalName()).append("   [file, inode=").append(f.getId())
                .append(", repl=").append(f.getFileReplication())
                .append(", blockSize=").append(f.getPreferredBlockSize())
                .append(", len=").append(f.computeFileSize(true));
        if (f.isUnderConstruction()) {
            sb.append(", UNDER_CONSTRUCTION, leaseHolder=").append(f.getFileUnderConstructionFeature().getClientName());
        }
        sb.append("]\n");
        for (BlockInfo b : f.getBlocks()) {
            sb.append(indent).append("      ").append(b).append("  len=").append(b.getNumBytes())
                    .append("  ").append(b.getBlockUCState())
                    .append("  replicas=").append(b.getStorageInfos());
            BlockUnderConstructionFeature uc = b.getUnderConstructionFeature();
            if (uc != null) {
                sb.append("  pipeline=").append(uc.getExpectedStorageLocations());
                if (uc.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
                    sb.append("  recoveryId=").append(uc.getBlockRecoveryId())
                            .append("  primary=").append(uc.getPrimary());
                }
            }
            sb.append('\n');
        }
    }
}
