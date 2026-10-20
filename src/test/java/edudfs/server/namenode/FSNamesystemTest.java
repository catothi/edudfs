package edudfs.server.namenode;

import edudfs.common.HdfsConfig;
import edudfs.protocol.AlreadyBeingCreatedException;
import edudfs.protocol.Block;
import edudfs.protocol.DatanodeID;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.FileAlreadyExistsException;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LastBlockWithStatus;
import edudfs.protocol.LeaseExpiredException;
import edudfs.protocol.LocatedBlock;
import edudfs.protocol.RecoveryInProgressException;
import edudfs.server.blockmanagement.BlockInfo;
import edudfs.server.blockmanagement.DatanodeDescriptor;
import edudfs.server.blockmanagement.DatanodeStorageInfo;
import edudfs.server.common.HdfsServerConstants;
import edudfs.server.common.HdfsServerConstants.BlockUCState;
import edudfs.server.namenode.LeaseManager.Lease;
import edudfs.server.protocol.BlockCommand;
import edudfs.server.protocol.BlockRecoveryCommand;
import edudfs.server.protocol.BlockReportReplica;
import edudfs.server.protocol.DatanodeCommand;
import edudfs.server.protocol.DatanodeProtocol;
import edudfs.server.protocol.DatanodeStorage;
import edudfs.server.protocol.ReceivedDeletedBlockInfo;
import edudfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import edudfs.server.protocol.StorageBlockReport;
import edudfs.server.protocol.StorageReceivedDeletedBlocks;
import edudfs.server.protocol.StorageReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FSNamesystem without network: DataNodes are only "simulated" by having the test
 * call registration, heartbeats and block reports directly.
 */
class FSNamesystemTest {

    private static final long BS = 1024;
    private static final short REPL = 3;
    private static final String BPID = HdfsServerConstants.BLOCK_POOL_ID;
    private static final long GB = 1L << 30;

    private FSNamesystem ns;
    private final DatanodeID dn1 = new DatanodeID("uuid-1", "localhost", 1, 11);
    private final DatanodeID dn2 = new DatanodeID("uuid-2", "localhost", 2, 12);
    private final DatanodeID dn3 = new DatanodeID("uuid-3", "localhost", 3, 13);

    @BeforeEach
    void setUp() {
        ns = new FSNamesystem();  // without monitor threads -> deterministic
    }

    /** Registration + first heartbeat (each DataNode reports one storage with 1 GB free). */
    private void registerDataNodes() {
        for (DatanodeID dn : List.of(dn1, dn2, dn3)) {
            ns.registerDatanode(dn);
            heartbeat(dn);
        }
    }

    /** Default storage of a simulated DataNode. */
    private static DatanodeStorage storage(DatanodeID dn) {
        return storage(dn, "a");
    }

    private static DatanodeStorage storage(DatanodeID dn, String volume) {
        return new DatanodeStorage("DS-" + dn.getDatanodeUuid() + "-" + volume);
    }

    private static StorageReport report(DatanodeStorage s, long remaining) {
        return new StorageReport(s, false, GB, 0, remaining);
    }

    private DatanodeCommand[] heartbeat(DatanodeID dn) {
        return ns.handleHeartbeat(dn, new StorageReport[]{report(storage(dn), GB)}, 0);
    }

    private HdfsFileStatus create(String src, String client) throws IOException {
        return ns.startFile(src, client, "host", false, REPL, BS);
    }

    private void received(DatanodeID dn, Block b) throws IOException {
        received(dn, storage(dn), b);
    }

    private void received(DatanodeID dn, DatanodeStorage s, Block b) throws IOException {
        ns.processIncrementalBlockReport(dn, BPID, new StorageReceivedDeletedBlocks[]{
                new StorageReceivedDeletedBlocks(s, new ReceivedDeletedBlockInfo[]{
                        new ReceivedDeletedBlockInfo(b, BlockStatus.RECEIVED_BLOCK)})});
    }

    private DatanodeDescriptor descriptor(DatanodeID id) {
        return ns.getBlockManager().getDatanodeManager().getDatanode(id);
    }

    private BlockInfo stored(Block b) {
        return ns.getBlockManager().getStoredBlock(b);
    }

    private INodeFile file(String src) throws IOException {
        return ns.getFSDirectory().getINodesInPath(src).getLastINode().asFile();
    }

    // ================================================================ create
    @Test
    void createGrantsLeaseAndCreatesParents() throws IOException {
        HdfsFileStatus st = create("/a/b/f", "A");
        assertTrue(st.isUnderConstruction());
        assertEquals(16388, st.getFileId());  // a=16386, b=16387
        Lease lease = ns.getLeaseManager().getLease("A");
        assertNotNull(lease);
        assertEquals(List.of(st.getFileId()), List.copyOf(lease.getFiles()));
    }

    @Test
    void oneLeasePerClientCoversAllItsFiles() throws IOException {
        create("/f1", "A");
        create("/f2", "A");
        assertEquals(1, ns.getLeaseManager().getLeases().size());
        assertEquals(2, ns.getLeaseManager().getLease("A").getFiles().size());
    }

    @Test
    void secondWriterIsRejectedWhileLeaseIsValid() throws IOException {
        create("/f", "A");
        AlreadyBeingCreatedException e = assertThrows(AlreadyBeingCreatedException.class, () -> create("/f", "B"));
        assertTrue(e.getMessage().contains("already being created by A"));
    }

    @Test
    void sameClientCannotRecreateItsOpenFile() throws IOException {
        create("/f", "A");
        AlreadyBeingCreatedException e = assertThrows(AlreadyBeingCreatedException.class, () -> create("/f", "A"));
        assertTrue(e.getMessage().contains("current leaseholder is trying to recreate file"));
    }

    @Test
    void createOnClosedFileWithoutOverwriteFails() throws IOException {
        HdfsFileStatus st = create("/f", "A");
        assertTrue(ns.completeFile("/f", "A", null, st.getFileId()));
        assertThrows(FileAlreadyExistsException.class, () -> create("/f", "B"));
    }

    @Test
    void overwriteDeletesOpenFileAndItsLease() throws IOException {
        HdfsFileStatus old = create("/f", "A");
        HdfsFileStatus newStatus = ns.startFile("/f", "B", "host", true, REPL, BS);
        assertTrue(newStatus.getFileId() != old.getFileId());
        assertNull(ns.getLeaseManager().getLease("A"));
        // The old writer no longer finds its file (by fileId)
        assertThrows(FileNotFoundException.class, () -> ns.completeFile("/f", "A", null, old.getFileId()));
    }

    @Test
    void createOnDirectoryFails() throws IOException {
        ns.mkdirs("/d");
        assertThrows(FileAlreadyExistsException.class, () -> create("/d", "A"));
    }

    @Test
    void invalidBlockSizeIsRejected() {
        assertThrows(IOException.class, () -> ns.startFile("/f", "A", "host", false, REPL, 1000));
    }

    // ============================================================== complete
    @Test
    void completeEmptyFileRemovesLease() throws IOException {
        HdfsFileStatus st = create("/f", "A");
        assertTrue(ns.completeFile("/f", "A", null, st.getFileId()));
        assertFalse(file("/f").isUnderConstruction());
        assertTrue(ns.getLeaseManager().getLeases().isEmpty());
        // retrying complete() is idempotent
        assertTrue(ns.completeFile("/f", "A", null, st.getFileId()));
    }

    @Test
    void onlyTheLeaseHolderMayComplete() throws IOException {
        HdfsFileStatus st = create("/f", "A");
        assertThrows(LeaseExpiredException.class, () -> ns.completeFile("/f", "B", null, st.getFileId()));
    }

    @Test
    void writesFollowTheInodeAcrossRenames() throws IOException {
        HdfsFileStatus st = create("/f", "A");
        assertTrue(ns.rename("/f", "/renamed"));
        assertTrue(ns.completeFile("/f", "A", null, st.getFileId()));  // old path does not matter, fileId counts
        assertFalse(file("/renamed").isUnderConstruction());
    }

    // ============================================================== addBlock
    @Test
    void addBlockNeedsLiveDataNodes() throws IOException {
        HdfsFileStatus st = create("/f", "A");
        IOException e = assertThrows(IOException.class,
                () -> ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null));
        assertTrue(e.getMessage().contains("could only be written to 0"));
    }

    @Test
    void addBlockAllocatesSequentialIdsAndPipeline() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        LocatedBlock lb = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null);
        assertEquals(1073741825L, lb.getBlock().getBlockId());
        assertEquals(1001, lb.getBlock().getGenerationStamp());
        assertEquals(0, lb.getStartOffset());
        assertEquals(3, lb.getLocations().length);
        BlockInfo bi = stored(lb.getBlock());
        assertEquals(BlockUCState.UNDER_CONSTRUCTION, bi.getBlockUCState());
        assertEquals(st.getFileId(), bi.getBlockCollectionId());
        assertEquals(3, bi.getUnderConstructionFeature().getNumExpectedLocations());
    }

    @Test
    void addBlockRespectsExcludedNodes() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        LocatedBlock lb = ns.getAdditionalBlock("/f", st.getFileId(), "A", null,
                new DatanodeInfo[]{new DatanodeInfo(dn2)});
        assertEquals(2, lb.getLocations().length);
        for (DatanodeInfo dn : lb.getLocations()) {
            assertFalse(dn.equals(dn2));
        }
    }

    @Test
    void addBlockByForeignClientFails() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        assertThrows(LeaseExpiredException.class, () -> ns.getAdditionalBlock("/f", st.getFileId(), "B", null, null));
    }

    // ================================================================ append
    /** Write a file with one finished block (len bytes) and close it. */
    private Block writeClosedFile(String src, String client, int len) throws IOException {
        HdfsFileStatus st = create(src, client);
        Block b = ns.getAdditionalBlock(src, st.getFileId(), client, null, null).getBlock();
        b.setNumBytes(len);
        received(dn1, b);
        assertTrue(ns.completeFile(src, client, b, st.getFileId()));
        return b;
    }

    @Test
    void appendReopensClosedFileAndAddsNewBlockBehindLast() throws IOException {
        registerDataNodes();
        Block first = writeClosedFile("/f", "A", 700);

        LastBlockWithStatus lbs = ns.appendFile("/f", "B", "host");
        assertEquals(first, lbs.getLastBlock().getBlock());
        assertEquals(0, lbs.getLastBlock().getStartOffset());
        assertTrue(lbs.getFileStatus().isUnderConstruction());
        assertEquals("B", file("/f").getFileUnderConstructionFeature().getClientName());
        assertNotNull(ns.getLeaseManager().getLease("B"));

        // NEW_BLOCK: the old block stays COMPLETE, the new one starts at offset 700
        long fileId = lbs.getFileStatus().getFileId();
        LocatedBlock lb = ns.getAdditionalBlock("/f", fileId, "B", first, null);
        assertEquals(700, lb.getStartOffset());
        assertEquals(BlockUCState.COMPLETE, stored(first).getBlockUCState());
        Block second = lb.getBlock();
        second.setNumBytes(300);
        received(dn1, second);
        assertTrue(ns.completeFile("/f", "B", second, fileId));
        assertEquals(1000, ns.getFileInfo("/f").getLen());
        assertEquals(2, file("/f").numBlocks());
        assertNull(ns.getLeaseManager().getLease("B"));
    }

    @Test
    void appendWithoutWritingClosesFileUnchanged() throws IOException {
        registerDataNodes();
        Block first = writeClosedFile("/f", "A", 700);
        LastBlockWithStatus lbs = ns.appendFile("/f", "A", "host");
        assertTrue(ns.completeFile("/f", "A", first, lbs.getFileStatus().getFileId()));
        assertEquals(700, ns.getFileInfo("/f").getLen());
        assertFalse(ns.getFileInfo("/f").isUnderConstruction());
    }

    @Test
    void appendOnOpenFileIsRejected() throws IOException {
        create("/f", "A");
        AlreadyBeingCreatedException e = assertThrows(AlreadyBeingCreatedException.class,
                () -> ns.appendFile("/f", "B", "host"));
        assertTrue(e.getMessage().contains("APPEND_FILE"));
        assertTrue(e.getMessage().contains("already being created by A"));
    }

    @Test
    void appendOnMissingFileOrDirectoryFails() throws IOException {
        ns.mkdirs("/d");
        assertThrows(FileNotFoundException.class, () -> ns.appendFile("/missing", "A", "host"));
        assertThrows(FileNotFoundException.class, () -> ns.appendFile("/d", "A", "host"));
    }

    // ========================================== commit / complete / reports
    @Test
    void blockBecomesCompleteAfterCommitAndFirstReplica() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        b.setNumBytes(700);

        // commit before any report: COMMITTED, complete() returns false (client must retry)
        assertFalse(ns.completeFile("/f", "A", b, st.getFileId()));
        assertEquals(BlockUCState.COMMITTED, stored(b).getBlockUCState());
        assertEquals(700, stored(b).getNumBytes());

        // first FINALIZED report -> COMPLETE
        received(dn1, b);
        assertEquals(BlockUCState.COMPLETE, stored(b).getBlockUCState());
        assertTrue(ns.completeFile("/f", "A", b, st.getFileId()));
        assertEquals(700, ns.getFileInfo("/f").getLen());

        received(dn2, b);
        received(dn3, b);
        assertEquals(3, stored(b).numNodes());
        assertEquals(3, ns.getBlockLocations("/f", 0, Long.MAX_VALUE).getLocatedBlocks().get(0)
                .getLocations().length);
    }

    @Test
    void secondBlockCommitsTheFirst() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b1 = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        b1.setNumBytes(BS);
        LocatedBlock lb2 = ns.getAdditionalBlock("/f", st.getFileId(), "A", b1, null);
        assertEquals(BlockUCState.COMMITTED, stored(b1).getBlockUCState());
        assertEquals(BS, lb2.getStartOffset());
        assertEquals(1073741826L, lb2.getBlock().getBlockId());
        assertEquals(1002, lb2.getBlock().getGenerationStamp());
    }

    @Test
    void commitWithWrongGenerationStampFails() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        Block wrong = new Block(b.getBlockId(), 10, b.getGenerationStamp() + 5);
        assertThrows(IOException.class, () -> ns.completeFile("/f", "A", wrong, st.getFileId()));
    }

    @Test
    void reportsOfUnknownOrStaleReplicasAreInvalidated() throws IOException {
        registerDataNodes();
        received(dn1, new Block(4242, 10, 1001));  // belongs to no file
        assertTrue(descriptor(dn1).getPendingInvalidations().contains(new Block(4242, 0, 0)));

        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        received(dn2, new Block(b.getBlockId(), 0, b.getGenerationStamp() - 1));  // stale GS
        assertTrue(descriptor(dn2).getPendingInvalidations().contains(b));
        assertEquals(0, stored(b).numNodes());
    }

    @Test
    void replicaWithWrongLengthIsTreatedAsCorrupt() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        b.setNumBytes(500);
        received(dn1, b);
        ns.completeFile("/f", "A", b, st.getFileId());
        received(dn2, new Block(b.getBlockId(), 499, b.getGenerationStamp()));
        assertEquals(1, stored(b).numNodes());
        assertTrue(descriptor(dn2).getPendingInvalidations().contains(b));
    }

    @Test
    void fullBlockReportRemovesReplicasNoLongerReported() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        b.setNumBytes(100);
        received(dn1, b);
        received(dn2, b);
        received(dn3, b);
        ns.completeFile("/f", "A", b, st.getFileId());
        assertEquals(3, stored(b).numNodes());
        ns.processReport(dn2, BPID, new StorageBlockReport[]{
                new StorageBlockReport(storage(dn2), new BlockReportReplica[0])});  // dn2 has lost the block
        assertEquals(2, stored(b).numNodes());
        assertTrue(ns.getBlockManager().getNeededReconstruction().contains(stored(b)));
    }

    // ============================================================ Heartbeats
    @Test
    void unknownDataNodeIsAskedToRegister() {
        DatanodeCommand[] cmds = heartbeat(new DatanodeID("unknown", "localhost", 9, 19));
        assertEquals(1, cmds.length);
        assertEquals(DatanodeProtocol.DNA_REGISTER, cmds[0].getAction());
    }

    @Test
    void deleteQueuesInvalidationsDeliveredWithHeartbeat() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        b.setNumBytes(100);
        received(dn1, b);
        ns.completeFile("/f", "A", b, st.getFileId());

        assertTrue(ns.delete("/f", false));
        assertNull(stored(b));
        assertNull(ns.getFileInfo("/f"));

        DatanodeCommand[] cmds = heartbeat(dn1);
        BlockCommand inv = (BlockCommand) cmds[cmds.length - 1];
        assertEquals(DatanodeProtocol.DNA_INVALIDATE, inv.getAction());
        assertEquals(b, inv.getBlocks()[0]);
        assertEquals(0, heartbeat(dn1).length);  // queue is now empty
    }

    @Test
    void nonEmptyDirectoryNeedsRecursiveDelete() throws IOException {
        create("/d/f", "A");
        assertThrows(IOException.class, () -> ns.delete("/d", false));
        assertTrue(ns.delete("/d", true));
        assertTrue(ns.getLeaseManager().getLeases().isEmpty());  // lease of the open file is gone
    }

    // ======================================================== lease recovery
    @Test
    void recoverLeaseOnEmptyFileClosesImmediately() throws IOException {
        create("/f", "A");
        assertTrue(ns.recoverLease("/f", "B"));
        assertFalse(file("/f").isUnderConstruction());
    }

    @Test
    void softLimitExpiredAllowsTakeoverOfEmptyFile() throws Exception {
        create("/f", "A");
        Thread.sleep(HdfsConfig.LEASE_SOFTLIMIT_MS + 300);
        // recovery closes the empty file immediately -> afterwards "already exists" applies
        assertThrows(FileAlreadyExistsException.class, () -> create("/f", "B"));
        assertFalse(file("/f").isUnderConstruction());
    }

    @Test
    void blockRecoveryEndToEndOnNameNodeSide() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();

        // 1) forced recovery: lease -> HDFS_NameNode, block -> UNDER_RECOVERY, new GS
        assertFalse(ns.recoverLease("/f", "B"));
        INodeFile f = file("/f");
        assertEquals(HdfsServerConstants.NAMENODE_LEASE_HOLDER, f.getFileUnderConstructionFeature().getClientName());
        assertNull(ns.getLeaseManager().getLease("A"));
        BlockInfo bi = stored(b);
        assertEquals(BlockUCState.UNDER_RECOVERY, bi.getBlockUCState());
        long recoveryId = bi.getUnderConstructionFeature().getBlockRecoveryId();
        assertEquals(b.getGenerationStamp() + 1, recoveryId);

        // the old writer no longer has any rights
        Block commit = new Block(b);
        assertThrows(LeaseExpiredException.class, () -> ns.completeFile("/f", "A", commit, st.getFileId()));

        // 2) the primary receives the command with the next heartbeat
        DatanodeID primary = bi.getUnderConstructionFeature().getPrimary();
        BlockRecoveryCommand cmd = null;
        for (DatanodeCommand c : heartbeat(primary)) {
            if (c.getAction() == DatanodeProtocol.DNA_RECOVERBLOCK) {
                cmd = (BlockRecoveryCommand) c;
            }
        }
        assertNotNull(cmd);
        assertEquals(recoveryId, cmd.getRecoveringBlocks().get(0).getNewGenerationStamp());
        assertEquals(3, cmd.getRecoveringBlocks().get(0).getLocations().length);

        // 3) a wrong recoveryId is rejected
        String[] storages = {storage(dn1).getStorageID(), storage(dn2).getStorageID()};
        assertThrows(IOException.class, () -> ns.commitBlockSynchronization(b, recoveryId + 7, 300, true, false,
                new DatanodeID[]{dn1, dn2}, storages));

        // 4) result: 300 bytes on dn1 and dn2 -> file closed
        ns.commitBlockSynchronization(b, recoveryId, 300, true, false, new DatanodeID[]{dn1, dn2}, storages);
        assertFalse(f.isUnderConstruction());
        assertEquals(BlockUCState.COMPLETE, bi.getBlockUCState());
        assertEquals(recoveryId, bi.getGenerationStamp());
        assertEquals(300, ns.getFileInfo("/f").getLen());
        assertEquals(2, bi.numNodes());
        assertTrue(ns.getLeaseManager().getLeases().isEmpty());
    }

    @Test
    void blockRecoveryWithoutAnyReplicaDropsTheBlock() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        assertFalse(ns.recoverLease("/f", "B"));
        long recoveryId = stored(b).getUnderConstructionFeature().getBlockRecoveryId();
        ns.commitBlockSynchronization(b, recoveryId, 0, true, true, new DatanodeID[0], new String[0]);
        assertNull(stored(b));
        assertEquals(0, file("/f").numBlocks());
        assertFalse(file("/f").isUnderConstruction());
    }

    @Test
    void hardLimitMonitorRecoversWithoutAnyClient() throws Exception {
        create("/f", "A");
        Thread.sleep(HdfsConfig.LEASE_HARDLIMIT_MS + 300);
        ns.writeLock();
        try {
            ns.getLeaseManager().checkLeases();
        } finally {
            ns.writeUnlock();
        }
        assertFalse(file("/f").isUnderConstruction());
        assertTrue(ns.getLeaseManager().getLeases().isEmpty());
    }

    @Test
    void renewLeaseMovesLeaseToTheEndOfTheQueue() throws Exception {
        create("/f1", "A");
        Thread.sleep(5);
        create("/f2", "B");
        assertEquals("A", ns.getLeaseManager().getLeases().get(0).getHolder());
        Thread.sleep(5);
        ns.renewLease("A");
        assertEquals("B", ns.getLeaseManager().getLeases().get(0).getHolder());
    }

    @Test
    void reportedRbwReplicaOfBlockUnderConstructionBecomesExpectedLocation() throws IOException {
        registerDataNodes();
        DatanodeID dn4 = new DatanodeID("uuid-4", "localhost", 4, 14);
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null,
                new DatanodeInfo[]{new DatanodeInfo(dn3)}).getBlock();
        ns.registerDatanode(dn4);
        ns.processReport(dn4, BPID, new StorageBlockReport[]{new StorageBlockReport(storage(dn4),
                new BlockReportReplica[]{new BlockReportReplica(b, HdfsServerConstants.ReplicaState.RBW)})});
        assertTrue(stored(b).getUnderConstructionFeature().getExpectedLocations().contains(descriptor(dn4)));
        assertInstanceOf(DatanodeDescriptor.class, descriptor(dn4));
    }

    // ============================================================ Storages
    /** Block b on dn1 (storage s1), dn2, dn3 -> COMPLETE with 3 replicas. */
    private Block completeOn3(String src, DatanodeStorage onDn1) throws IOException {
        HdfsFileStatus st = create(src, "A");
        Block b = ns.getAdditionalBlock(src, st.getFileId(), "A", null, null).getBlock();
        b.setNumBytes(100);
        received(dn1, onDn1, b);
        received(dn2, b);
        received(dn3, b);
        assertTrue(ns.completeFile(src, "A", b, st.getFileId()));
        return b;
    }

    private DatanodeStorageInfo storageInfo(DatanodeID dn, DatanodeStorage s) {
        return descriptor(dn).getStorageInfo(s.getStorageID());
    }

    @Test
    void addBlockChoosesOneStoragePerDataNode() throws IOException {
        registerDataNodes();
        HdfsFileStatus st = create("/f", "A");
        Block b = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null).getBlock();
        List<DatanodeStorageInfo> pipeline = stored(b).getUnderConstructionFeature().getExpectedStorageLocations();
        assertEquals(3, pipeline.size());
        assertEquals(3, pipeline.stream().map(DatanodeStorageInfo::getDatanodeDescriptor).distinct().count());
        assertTrue(pipeline.contains(storageInfo(dn1, storage(dn1))));
    }

    @Test
    void storageWithoutSpaceIsNotChosen() throws IOException {
        registerDataNodes();
        ns.handleHeartbeat(dn3, new StorageReport[]{report(storage(dn3), BS - 1)}, 0);  // dn3: disk full
        HdfsFileStatus st = create("/f", "A");
        LocatedBlock lb = ns.getAdditionalBlock("/f", st.getFileId(), "A", null, null);
        assertEquals(2, lb.getLocations().length);
        assertFalse(Arrays.asList(lb.getLocations()).contains(dn3));
    }

    @Test
    void replicaOnAnotherStorageOfTheSameNodeReplacesTheOldEntry() throws IOException {
        registerDataNodes();
        DatanodeStorage a = storage(dn1, "a");
        DatanodeStorage b2 = storage(dn1, "b");
        Block b = completeOn3("/f", a);
        received(dn1, b2, b);  // e.g. found on the other disk after a restart
        assertEquals(3, stored(b).numNodes());  // only one replica counts per DataNode
        assertEquals(storageInfo(dn1, b2), stored(b).findStorageInfo(descriptor(dn1)));
        assertEquals(0, storageInfo(dn1, a).numBlocks());
        assertEquals(1, storageInfo(dn1, b2).numBlocks());
    }

    @Test
    void failedVolumeInHeartbeatRemovesOnlyThatStoragesReplicas() throws IOException {
        registerDataNodes();
        DatanodeStorage a = storage(dn1, "a");
        DatanodeStorage b2 = storage(dn1, "b");
        ns.handleHeartbeat(dn1, new StorageReport[]{report(a, GB), report(b2, GB)}, 0);
        Block onA = completeOn3("/fa", a);
        Block onB = completeOn3("/fb", b2);
        assertEquals(2, descriptor(dn1).getNumBlocks());

        // dn1 now reports only storage a and 1 failed volume -> b is FAILED
        ns.handleHeartbeat(dn1, new StorageReport[]{report(a, GB)}, 1);
        assertEquals(DatanodeStorage.State.FAILED, storageInfo(dn1, b2).getState());
        assertEquals(DatanodeStorage.State.NORMAL, storageInfo(dn1, a).getState());
        assertEquals(1, descriptor(dn1).getVolumeFailures());
        assertTrue(descriptor(dn1).isAlive());
        assertEquals(3, stored(onA).numNodes());
        assertEquals(2, stored(onB).numNodes());
        assertTrue(ns.getBlockManager().getNeededReconstruction().contains(stored(onB)));
        assertFalse(ns.getBlockManager().getNeededReconstruction().contains(stored(onA)));
    }

    @Test
    void storageMissingFromFullBlockReportIsPruned() throws IOException {
        registerDataNodes();
        DatanodeStorage a = storage(dn1, "a");
        DatanodeStorage b2 = storage(dn1, "b");
        ns.handleHeartbeat(dn1, new StorageReport[]{report(a, GB), report(b2, GB)}, 0);
        Block onB = completeOn3("/f", b2);
        ns.processReport(dn1, BPID, new StorageBlockReport[]{new StorageBlockReport(a, new BlockReportReplica[0])});
        assertNull(storageInfo(dn1, b2));
        assertEquals(1, descriptor(dn1).getStorageInfos().size());
        assertEquals(2, stored(onB).numNodes());
    }

    @Test
    void reportsForAForeignBlockPoolAreRejected() {
        ns.registerDatanode(dn1);
        assertThrows(IOException.class, () -> ns.processReport(dn1, "BP-other-127.0.0.1-1",
                new StorageBlockReport[]{new StorageBlockReport(storage(dn1), new BlockReportReplica[0])}));
    }
}
