package edudfs.tools;

import edudfs.client.DFSClient;
import edudfs.client.DFSInputStream;
import edudfs.client.DFSOutputStream;
import edudfs.common.HdfsConfig;
import edudfs.protocol.AlreadyBeingCreatedException;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.FileAlreadyExistsException;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LocatedBlock;
import edudfs.protocol.LocatedBlocks;
import edudfs.protocol.RecoveryInProgressException;
import edudfs.server.blockmanagement.DatanodeDescriptor;
import edudfs.server.common.HdfsServerConstants;
import edudfs.server.datanode.DataNode;
import edudfs.server.datanode.FsVolumeImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end tests: NameNode + 4 DataNodes in one JVM, real RMI and TCP connections.
 * The shortened timings (leases, heartbeats) come from the surefire configuration in pom.xml.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class MiniDFSClusterTest {

    private static final int BS = 128 * 1024;
    private static final short REPL = 3;

    @TempDir
    File baseDir;

    private MiniDFSCluster cluster;
    private DFSClient client;

    @BeforeEach
    void startCluster() throws Exception {
        cluster = new MiniDFSCluster(4, baseDir);
        cluster.waitActive();
        client = new DFSClient();
    }

    @AfterEach
    void stopCluster() {
        client.abort();
        cluster.close();
    }

    // ------------------------------------------------------------ helpers
    interface Condition {
        boolean holds() throws Exception;
    }

    private static void waitFor(String what, long timeoutMs, Condition c) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!c.holds()) {
            if (System.currentTimeMillis() > deadline) {
                fail("Timeout waiting for: " + what);
            }
            Thread.sleep(100);
        }
    }

    private static byte[] data(int n) {
        byte[] b = new byte[n];
        new Random(n).nextBytes(b);
        return b;
    }

    private void write(DFSClient c, String path, byte[] d) throws IOException {
        try (DFSOutputStream out = c.create(path, false, REPL, BS)) {
            out.write(d);
        }
    }

    private byte[] read(String path) throws IOException {
        try (DFSInputStream in = client.open(path)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            in.transferTo(bos);
            return bos.toByteArray();
        }
    }

    private List<LocatedBlock> blocks(String path) throws IOException {
        return client.getNamenode().getBlockLocations(path, 0, Long.MAX_VALUE).getLocatedBlocks();
    }

    private boolean isClosed(String path) throws IOException {
        HdfsFileStatus s = client.getFileInfo(path);
        return s != null && !s.isUnderConstruction();
    }

    private boolean allBlocksHaveReplicas(String path, int n) throws IOException {
        for (LocatedBlock lb : blocks(path)) {
            if (lb.getLocations().length != n) {
                return false;
            }
        }
        return true;
    }

    /** Writes and then lets the client "crash" (no close, no lease renewal). */
    private DFSClient writeAndCrash(String path, byte[] d) throws Exception {
        DFSClient c = new DFSClient();
        DFSOutputStream out = c.create(path, false, REPL, BS);
        out.write(d);
        Thread.sleep(500);  // let full packets go through the pipeline
        c.abort();
        return c;
    }

    // ============================================================ read/write
    @Test
    void writeAndReadBackVariousSizes() throws IOException {
        int[] sizes = {0, 1, 511, 512, 513, 65535, 65536, 65537, BS - 1, BS, BS + 1, 3 * BS + 100};
        for (int n : sizes) {
            String p = "/sizes/f" + n;
            byte[] d = data(n);
            write(client, p, d);
            assertArrayEquals(d, read(p), "size " + n);
            assertEquals(n, client.getFileInfo(p).getLen());
            assertEquals((n + BS - 1) / BS, blocks(p).size(), "blocks for size " + n);
        }
    }

    @Test
    void firstBlockOfFreshClusterHasWellKnownIdAndStamp() throws IOException {
        write(client, "/f", data(300 * 1024));
        List<LocatedBlock> lbs = blocks("/f");
        assertEquals(3, lbs.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(1073741825L + i, lbs.get(i).getBlock().getBlockId());
            assertEquals(1001 + i, lbs.get(i).getBlock().getGenerationStamp());
            assertEquals((long) i * BS, lbs.get(i).getStartOffset());
            assertEquals(3, lbs.get(i).getLocations().length);
        }
        assertEquals(300 * 1024 - 2 * BS, lbs.get(2).getBlock().getNumBytes());
        assertEquals(16386, client.getFileInfo("/f").getFileId());
    }

    @Test
    void appendAddsDataInANewBlock() throws IOException {
        byte[] a = data(1000);
        byte[] b = data(500);
        byte[] c = data(BS + 10);
        write(client, "/app", a);
        try (DFSOutputStream out = client.append("/app")) {
            out.write(b);
        }
        try (DFSOutputStream out = client.append("/app")) {
            out.write(c);
        }
        byte[] expected = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, expected, 0, a.length);
        System.arraycopy(b, 0, expected, a.length, b.length);
        System.arraycopy(c, 0, expected, a.length + b.length, c.length);
        assertArrayEquals(expected, read("/app"));

        // NEW_BLOCK semantics: 1000 | 500 | BS | 10 - blocks in the middle may be shorter
        List<LocatedBlock> lbs = blocks("/app");
        assertEquals(4, lbs.size());
        assertEquals(1000, lbs.get(0).getBlock().getNumBytes());
        assertEquals(500, lbs.get(1).getBlock().getNumBytes());
        assertEquals(1500, lbs.get(2).getStartOffset());
        assertTrue(isClosed("/app"));
    }

    @Test
    void readFromTheMiddleOfAFile() throws IOException {
        byte[] d = data(2 * BS + 999);
        write(client, "/f", d);
        try (DFSInputStream in = client.open("/f")) {
            in.skipNBytes(BS + 777);  // in the middle of block 2, not chunk-aligned
            byte[] rest = in.readAllBytes();
            assertArrayEquals(Arrays.copyOfRange(d, BS + 777, d.length), rest);
        }
    }

    @Test
    void replicationFactorOneIsHonoured() throws IOException {
        try (DFSOutputStream out = client.create("/single", false, (short) 1, BS)) {
            out.write(data(1000));
        }
        assertEquals(1, blocks("/single").get(0).getLocations().length);
    }

    // ================================================================ Namespace
    @Test
    void namespaceOperations() throws IOException {
        assertTrue(client.mkdirs("/a/b/c"));
        write(client, "/a/b/f2", data(10));
        write(client, "/a/b/f1", data(10));
        List<String> names = new ArrayList<>();
        for (HdfsFileStatus s : client.listPaths("/a/b")) {
            names.add(s.getPath());
        }
        assertEquals(List.of("/a/b/c", "/a/b/f1", "/a/b/f2"), names);  // sorted

        long id = client.getFileInfo("/a/b/f1").getFileId();
        assertTrue(client.rename("/a/b/f1", "/a/moved"));
        assertEquals(id, client.getFileInfo("/a/moved").getFileId());
        assertArrayEquals(data(10), read("/a/moved"));

        assertThrows(IOException.class, () -> client.delete("/a", false));
        assertTrue(client.delete("/a", true));
        assertNull(client.getFileInfo("/a"));
        assertFalse(client.delete("/a", true));
    }

    @Test
    void renameWhileWritingDoesNotBreakTheWriter() throws IOException {
        byte[] d = data(BS + 5000);
        try (DFSOutputStream out = client.create("/open", false, REPL, BS)) {
            out.write(d, 0, 70000);
            assertTrue(client.rename("/open", "/renamed"));
            out.write(d, 70000, d.length - 70000);
        }
        assertArrayEquals(d, read("/renamed"));
    }

    @Test
    void secondWriterGetsAlreadyBeingCreated() throws IOException {
        try (DFSOutputStream ignored = client.create("/f", false, REPL, BS);
             DFSClient other = new DFSClient()) {
            assertThrows(AlreadyBeingCreatedException.class, () -> other.create("/f", false, REPL, BS));
        }
    }

    // =================================================================== Leases
    @Test
    void softLimitLetsAnotherClientTriggerRecovery() throws Exception {
        byte[] d = data(200 * 1024);
        writeAndCrash("/soft", d);
        try (DFSClient other = new DFSClient()) {
            assertThrows(AlreadyBeingCreatedException.class, () -> other.create("/soft", false, REPL, BS));
            Thread.sleep(HdfsConfig.LEASE_SOFTLIMIT_MS + 300);
            IOException e = assertThrows(IOException.class, () -> other.create("/soft", false, REPL, BS));
            assertInstanceOf(RecoveryInProgressException.class, e);
            waitFor("file closed", 10000, () -> isClosed("/soft"));
            assertThrows(FileAlreadyExistsException.class, () -> other.create("/soft", false, REPL, BS));
        }
        // 128 KB block 1 + one sent 64 KB packet of block 2; 8 KB were in the client buffer
        assertEquals(BS + 65536, client.getFileInfo("/soft").getLen());
        assertArrayEquals(Arrays.copyOf(d, BS + 65536), read("/soft"));
        List<LocatedBlock> lbs = blocks("/soft");
        assertTrue(lbs.get(1).getBlock().getGenerationStamp() > lbs.get(0).getBlock().getGenerationStamp() + 1,
                "recovered block must carry the new generation stamp (recoveryId)");
    }

    @Test
    void hardLimitRecoveryClosesAbandonedFile() throws Exception {
        byte[] d = data(100 * 1024);
        writeAndCrash("/hard", d);
        assertFalse(isClosed("/hard"));
        waitFor("hard-limit recovery", HdfsConfig.LEASE_HARDLIMIT_MS + 10000, () -> isClosed("/hard"));
        assertEquals(65536, client.getFileInfo("/hard").getLen());
        assertArrayEquals(Arrays.copyOf(d, 65536), read("/hard"));
    }

    @Test
    void forcedRecoverLease() throws Exception {
        writeAndCrash("/forced", data(70000));
        assertFalse(client.recoverLease("/forced"));  // block recovery triggered
        waitFor("recoverLease true", 10000, () -> client.recoverLease("/forced"));
        assertEquals(65536, client.getFileInfo("/forced").getLen());
    }

    @Test
    void originalWriterLosesItsLeaseAfterRecovery() throws Exception {
        DFSClient writer = new DFSClient();
        DFSOutputStream out = writer.create("/stolen", false, REPL, BS);
        out.write(data(200 * 1024));
        Thread.sleep(500);
        assertFalse(client.recoverLease("/stolen"));  // forced despite a live writer
        waitFor("file closed", 10000, () -> isClosed("/stolen"));
        assertThrows(IOException.class, out::close);  // writer stopped / LeaseExpired
        writer.abort();
    }

    @Test
    void leaseIsRenewedWhileClientIsAlive() throws Exception {
        try (DFSOutputStream out = client.create("/long", false, REPL, BS);
             DFSClient other = new DFSClient()) {
            out.write(data(1000));
            Thread.sleep(HdfsConfig.LEASE_SOFTLIMIT_MS * 2);  // well beyond the soft limit
            // LeaseRenewer has renewed -> still "already being created", no recovery
            assertThrows(AlreadyBeingCreatedException.class, () -> other.create("/long", false, REPL, BS));
        }
        assertEquals(1000, client.getFileInfo("/long").getLen());
    }

    // ============================================================ fault tolerance
    @Test
    void deadDataNodeTriggersReReplication() throws Exception {
        byte[] d = data(3 * BS);
        write(client, "/f", d);
        int victimPort = blocks("/f").get(0).getLocations()[0].getXferPort();
        cluster.stopDataNode(victimPort - MiniDFSCluster.BASE_XFER_PORT);

        waitFor("dead node detected and blocks re-replicated", 20000, () -> {
            for (LocatedBlock lb : blocks("/f")) {
                if (lb.getLocations().length != 3) {
                    return false;
                }
                for (DatanodeInfo dn : lb.getLocations()) {
                    if (dn.getXferPort() == victimPort) {
                        return false;
                    }
                }
            }
            return true;
        });
        assertArrayEquals(d, read("/f"));
        long dead = Arrays.stream(client.datanodeReport()).filter(dn -> !dn.isAlive()).count();
        assertEquals(1, dead);
    }

    @Test
    void corruptReplicaIsDetectedReportedAndReplaced() throws Exception {
        byte[] d = data(BS);
        write(client, "/f", d);
        LocatedBlock lb = blocks("/f").get(0);
        DatanodeInfo victim = lb.getLocations()[0];
        File blockFile = cluster.getDataNodeByXferPort(victim.getXferPort()).getBlockFile(lb.getBlock().getBlockId());
        try (RandomAccessFile raf = new RandomAccessFile(blockFile, "rw")) {
            raf.seek(4321);
            int b = raf.read();
            raf.seek(4321);
            raf.write(b ^ 0xFF);
        }
        assertArrayEquals(d, read("/f"));  // client falls back to another replica
        // The corrupt replica is deleted and re-replicated. The target is random and may
        // be the victim again (once its copy is gone) - what matters is: 3 intact copies.
        waitFor("corrupt replica replaced", 15000, () -> {
            LocatedBlock now = blocks("/f").get(0);
            if (now.getLocations().length != 3) {
                return false;
            }
            for (DatanodeInfo dn : now.getLocations()) {
                File f = cluster.getDataNodeByXferPort(dn.getXferPort()).getBlockFile(now.getBlock().getBlockId());
                if (f == null || !Arrays.equals(d, java.nio.file.Files.readAllBytes(f.toPath()))) {
                    return false;
                }
            }
            return true;
        });
        assertArrayEquals(d, read("/f"));
    }

    @Test
    void pipelineSetupFailureAbandonsBlockAndExcludesNode() throws Exception {
        // DataNode gone, but the NameNode still considers it alive -> ends up in pipelines
        cluster.stopDataNode(0);
        for (int i = 0; i < 6; i++) {
            byte[] d = data(BS + i);
            write(client, "/p" + i, d);
            assertArrayEquals(d, read("/p" + i));
            for (LocatedBlock lb : blocks("/p" + i)) {
                for (DatanodeInfo dn : lb.getLocations()) {
                    assertFalse(dn.getXferPort() == MiniDFSCluster.BASE_XFER_PORT, "dead node in locations");
                }
            }
        }
    }

    @Test
    void deleteRemovesBlockFilesFromDataNodes() throws Exception {
        write(client, "/f", data(2 * BS));
        List<File> files = new ArrayList<>();
        for (LocatedBlock lb : blocks("/f")) {
            for (DatanodeInfo dn : lb.getLocations()) {
                files.add(cluster.getDataNodeByXferPort(dn.getXferPort()).getBlockFile(lb.getBlock().getBlockId()));
            }
        }
        assertEquals(6, files.size());
        assertTrue(files.stream().allMatch(File::exists));
        assertTrue(client.delete("/f", false));
        waitFor("block files deleted", 10000, () -> files.stream().noneMatch(File::exists));
    }

    @Test
    void restartedDataNodeReportsItsReplicasAgain() throws Exception {
        byte[] d = data(BS);
        write(client, "/f", d);
        int port = blocks("/f").get(0).getLocations()[0].getXferPort();
        int idx = port - MiniDFSCluster.BASE_XFER_PORT;
        cluster.stopDataNode(idx);
        cluster.startDataNode(idx);  // same disk, same UUID
        waitFor("replica reported again", 10000, () -> Arrays.stream(blocks("/f").get(0).getLocations())
                .anyMatch(dn -> dn.getXferPort() == port));
        waitFor("exactly 3 replicas", 10000, () -> allBlocksHaveReplicas("/f", 3));
        assertArrayEquals(d, read("/f"));
    }

    @Test
    void nameNodeRestartLosesNamespaceAndOrphanBlocksAreDeleted() throws Exception {
        write(client, "/f", data(BS));
        LocatedBlock lb = blocks("/f").get(0);
        DataNode holder = cluster.getDataNodeByXferPort(lb.getLocations()[0].getXferPort());
        File blockFile = holder.getBlockFile(lb.getBlock().getBlockId());
        assertTrue(blockFile.exists());

        cluster.restartNameNode();
        client.abort();
        client = new DFSClient();
        assertNull(client.getFileInfo("/f"));  // no persistence
        waitFor("datanodes re-registered", 10000,
                () -> Arrays.stream(client.datanodeReport()).filter(DatanodeInfo::isAlive).count() == 4);
        waitFor("orphan block deleted", 10000, () -> !blockFile.exists());
    }

    @Test
    void readFailsCleanlyWhenAllReplicasAreGone() throws Exception {
        try (DFSOutputStream out = client.create("/one", false, (short) 1, BS)) {
            out.write(data(1000));
        }
        int port = blocks("/one").get(0).getLocations()[0].getXferPort();
        try (DFSInputStream in = client.open("/one")) {
            cluster.stopDataNode(port - MiniDFSCluster.BASE_XFER_PORT);
            IOException e = assertThrows(IOException.class, in::readAllBytes);
            assertTrue(e.getMessage().contains("Could not obtain block"), e.getMessage());
        }
    }

    @Test
    void locatedBlocksOfOpenFileExposeOnlyCompletedData() throws Exception {
        try (DFSOutputStream out = client.create("/open", false, REPL, BS)) {
            out.write(data(BS + 70000));
            Thread.sleep(500);
            LocatedBlocks lbs = client.getNamenode().getBlockLocations("/open", 0, Long.MAX_VALUE);
            assertTrue(lbs.isUnderConstruction());
            assertEquals(BS, lbs.getFileLength());
        }
        assertEquals(BS + 70000, client.getFileInfo("/open").getLen());
    }

    // ---------------------------------------------------- volumes (disks)
    private DatanodeDescriptor descriptorOf(DataNode dn) {
        return cluster.getNameNode().getNamesystem().getBlockManager().getDatanodeManager()
                .getDatanode(dn.getDatanodeId());
    }

    /** Volume on which DataNode dn holds the replica of block blockId. */
    private static FsVolumeImpl volumeOf(DataNode dn, long blockId) {
        return dn.getFSDataset().getReplica(blockId).getVolume();
    }

    @Test
    void replicasAreStoredInBlockPoolSubdirsOnAllVolumes() throws Exception {
        assumeTrue(HdfsConfig.DATANODE_DATA_DIRS.size() >= 2, "needs >= 2 volumes in hdfs-site.xml");
        write(client, "/f", data(4 * BS));  // 4 blocks x 3 replicas on 4 DataNodes
        Set<File> volumesUsed = new HashSet<>();
        for (LocatedBlock lb : blocks("/f")) {
            for (DatanodeInfo info : lb.getLocations()) {
                int idx = info.getXferPort() - MiniDFSCluster.BASE_XFER_PORT;
                DataNode dn = cluster.getDataNode(idx);
                File volumeDir = volumeOf(dn, lb.getBlock().getBlockId()).getStorageLocation().getDir();
                volumesUsed.add(volumeDir);
                // <host>/data/<n>/dfs/dn/current/<BP>/current/finalized/subdirX/subdirY/blk_<id>
                assertTrue(volumeDir.toPath().startsWith(cluster.getHostRoot(idx).toPath()), volumeDir.toString());
                File expected = new File(volumeDir, "current/" + HdfsServerConstants.BLOCK_POOL_ID
                        + "/current/finalized/subdir0/subdir0/blk_" + lb.getBlock().getBlockId());
                assertEquals(expected, dn.getBlockFile(lb.getBlock().getBlockId()));
                assertTrue(expected.exists());
            }
        }
        // 12 replicas on 4 DataNodes: at least one DataNode has several -> round-robin uses both disks
        assertTrue(volumesUsed.size() > 4, "volumes used: " + volumesUsed);
        for (int i = 0; i < 4; i++) {
            for (FsVolumeImpl v : cluster.getDataNode(i).getFSDataset().getVolumes()) {
                assertTrue(new File(v.getStorageLocation().getDir(), "current/VERSION").isFile());
            }
            assertEquals(HdfsConfig.DATANODE_DATA_DIRS.size(), descriptorOf(cluster.getDataNode(i))
                    .getStorageInfos().size());  // NameNode knows each storage individually
        }
    }

    @Test
    void failedVolumeIsDetectedAndOnlyItsReplicasAreReReplicated() throws Exception {
        assumeTrue(HdfsConfig.DATANODE_DATA_DIRS.size() >= 2 && HdfsConfig.FAILED_VOLUMES_TOLERATED >= 1,
                "needs >= 2 volumes and failed.volumes.tolerated >= 1");
        byte[] d = data(4 * BS);
        write(client, "/f", d);
        LocatedBlock lb = blocks("/f").get(0);
        DataNode victim = cluster.getDataNodeByXferPort(lb.getLocations()[0].getXferPort());
        FsVolumeImpl broken = volumeOf(victim, lb.getBlock().getBlockId());
        int replicasOnVictim = victim.getFSDataset().getNumBlocks();

        MiniDFSCluster.deleteRecursively(broken.getStorageLocation().getDir());  // disk fails

        DatanodeDescriptor desc = descriptorOf(victim);
        waitFor("volume failure reported", 10000, () -> desc.getVolumeFailures() == 1);
        waitFor("failed storage pruned", 10000, () -> desc.getStorageInfo(broken.getStorageID()) == null);
        assertTrue(desc.isAlive());  // DataNode keeps running with the remaining disk
        assertEquals(1, victim.getFSDataset().getVolumes().size());
        assertTrue(victim.getFSDataset().getNumBlocks() < replicasOnVictim);
        waitFor("all blocks have 3 replicas again", 20000, () -> allBlocksHaveReplicas("/f", 3));
        assertArrayEquals(d, read("/f"));
    }

    @Test
    void tooManyFailedVolumesShutDownTheDataNode() throws Exception {
        byte[] d = data(BS);
        write(client, "/f", d);
        DataNode victim = cluster.getDataNodeByXferPort(blocks("/f").get(0).getLocations()[0].getXferPort());
        for (FsVolumeImpl v : victim.getFSDataset().getVolumes()) {
            MiniDFSCluster.deleteRecursively(v.getStorageLocation().getDir());
        }
        DatanodeDescriptor desc = descriptorOf(victim);
        waitFor("datanode declared dead", 15000, () -> !desc.isAlive());
        waitFor("block re-replicated", 20000, () -> allBlocksHaveReplicas("/f", 3));
        assertArrayEquals(d, read("/f"));
    }

    // ====================================================== seek/pread, hflush
    @Test
    void preadAndSeekAcrossBlockBoundaries() throws IOException {
        byte[] d = data(3 * BS + 1000);
        write(client, "/f", d);
        try (DFSInputStream in = client.open("/f")) {
            byte[] b = new byte[5000];
            in.readFully(BS - 2000, b, 0, b.length);  // spans block 0 -> block 1
            assertArrayEquals(Arrays.copyOfRange(d, BS - 2000, BS + 3000), b);
            assertEquals(0, in.getPos());  // pread does not move the position
            in.seek(3 * BS + 10);
            assertEquals(d[3 * BS + 10] & 0xff, in.read());
            assertEquals(3 * BS + 11, in.getPos());
        }
    }

    @Test
    void hflushedDataSurvivesWriterCrash() throws Exception {
        DFSClient writer = new DFSClient();
        DFSOutputStream out = writer.create("/wal", false, REPL, BS);
        byte[] d = data(1700);
        out.write(d, 0, 1000);
        out.hflush();                // 1000 bytes: one full chunk + a partial one
        out.write(d, 1000, 700);
        out.hflush();                // the partial chunk is sent again and overwritten
        writer.abort();              // crash: no close(), the file stays under construction
        waitFor("lease recovered", 20000, () -> client.recoverLease("/wal"));
        assertArrayEquals(d, read("/wal"));
    }

    @Test
    void readerSeesHflushedBytesOfAnOpenFile() throws Exception {
        DFSOutputStream out = client.create("/open", false, REPL, BS);
        byte[] d = data(BS + 700);  // one completed block + 700 bytes in the block being written
        out.write(d);
        out.hflush();
        assertArrayEquals(d, read("/open"));  // visible length of the last block comes from the DataNodes
        out.close();
    }
}
