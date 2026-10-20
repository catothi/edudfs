package edudfs.server.datanode;

import edudfs.common.DataChecksum;
import edudfs.common.HdfsConfig;
import edudfs.protocol.Block;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.StorageType;
import edudfs.server.common.HdfsServerConstants;
import edudfs.server.common.HdfsServerConstants.ReplicaState;
import edudfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import edudfs.server.protocol.BlockReportReplica;
import edudfs.server.protocol.ReplicaRecoveryInfo;
import edudfs.server.protocol.StorageBlockReport;
import edudfs.server.protocol.StorageReport;
import edudfs.tools.MiniDFSCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Replicas on disk: volumes, layout, states, deletion, disk failure, block recovery. */
class FsDatasetImplTest {

    @TempDir
    File tmp;

    /** Two volumes as in conf/hdfs-site.xml, here directly under the temp directory. */
    private List<StorageLocation> locations;
    private FsDatasetImpl ds;

    @BeforeEach
    void setUp() throws IOException {
        locations = StorageLocation.parseAll(List.of("[DISK]/data/1/dfs/dn", "/data/2/dfs/dn"), tmp);
        ds = newDataset(1);
    }

    private FsDatasetImpl newDataset(int volFailuresTolerated) throws IOException {
        return new FsDatasetImpl(locations, "uuid-test", volFailuresTolerated, "testDN");
    }

    /** &lt;volume&gt;/current/&lt;BP-ID&gt;/current of the volume that holds r. */
    private static File bpCurrent(ReplicaInfo r) {
        return bpCurrent(r.getVolume().getStorageLocation().getDir());
    }

    private static File bpCurrent(File volumeDir) {
        return new File(volumeDir, "current/" + HdfsServerConstants.BLOCK_POOL_ID + "/current");
    }

    private static List<BlockReportReplica> flatten(StorageBlockReport[] reports) {
        List<BlockReportReplica> r = new ArrayList<>();
        for (StorageBlockReport sbr : reports) {
            r.addAll(Arrays.asList(sbr.getBlocks()));
        }
        return r;
    }

    private static byte[] data(int n) {
        byte[] b = new byte[n];
        new Random(n).nextBytes(b);
        return b;
    }

    /** Writes data + CRCs into an RBW replica (like the BlockReceiver). */
    private ReplicaInfo writeRbw(Block b, byte[] bytes) throws IOException {
        ReplicaInfo r = ds.createRbw(b);
        try (FileOutputStream out = new FileOutputStream(ds.getBlockFile(r), true);
             DataOutputStream meta = new DataOutputStream(new FileOutputStream(ds.getMetaFile(r), true))) {
            out.write(bytes);
            for (int c : DataChecksum.computeChunkedSums(bytes, 0, bytes.length)) {
                meta.writeInt(c);
            }
        }
        r.setNumBytes(bytes.length);
        return r;
    }

    private ReplicaInfo writeFinalized(Block b, byte[] bytes) throws IOException {
        ReplicaInfo r = writeRbw(b, bytes);
        ds.finalizeBlock(r);
        return r;
    }

    private static RecoveringBlock recovering(Block b, long newGs) {
        return new RecoveringBlock(b, new DatanodeInfo[0], newGs);
    }

    // ------------------------------------------------------------- Layout
    @Test
    void rbwReplicaLivesInRbwDirectoryWithMetaHeader() throws IOException {
        ReplicaInfo r = ds.createRbw(new Block(1073741825L, 0, 1001));
        assertEquals(ReplicaState.RBW, r.getState());
        assertEquals(new File(bpCurrent(r), "rbw/blk_1073741825"), ds.getBlockFile(r));
        File meta = ds.getMetaFile(r);
        assertEquals(new File(bpCurrent(r), "rbw/blk_1073741825_1001.meta"), meta);
        try (DataInputStream in = new DataInputStream(new FileInputStream(meta))) {
            assertEquals(1, in.readShort());                       // version
            assertEquals(DataChecksum.CHECKSUM_CRC32, in.readByte());
            assertEquals(HdfsConfig.BYTES_PER_CHECKSUM, in.readInt());
        }
        assertEquals(FsDatasetImpl.META_HEADER_SIZE, meta.length());
    }

    @Test
    void finalizeMovesFilesToFinalized() throws IOException {
        byte[] d = data(3000);
        ReplicaInfo r = writeFinalized(new Block(7, 0, 1001), d);
        assertEquals(ReplicaState.FINALIZED, r.getState());
        File blockFile = new File(bpCurrent(r), "finalized/subdir0/subdir0/blk_7");
        assertTrue(blockFile.exists());
        assertTrue(new File(bpCurrent(r), "finalized/subdir0/subdir0/blk_7_1001.meta").exists());
        assertFalse(new File(bpCurrent(r), "rbw/blk_7").exists());
        assertArrayEquals(d, Files.readAllBytes(blockFile.toPath()));
        assertEquals(FsDatasetImpl.META_HEADER_SIZE + 6 * 4, ds.getMetaFile(r).length());  // 3000 B = 6 chunks
    }

    @Test
    void finalizedReplicaCannotBeCreatedAgain() throws IOException {
        writeFinalized(new Block(7, 0, 1001), data(10));
        assertThrows(IOException.class, () -> ds.createRbw(new Block(7, 0, 1001)));
    }

    @Test
    void blockReportListsAllReplicasWithState() throws IOException {
        writeFinalized(new Block(1, 0, 1001), data(10));
        writeRbw(new Block(2, 0, 1002), data(20));
        StorageBlockReport[] reports = ds.getBlockReport();
        assertEquals(2, reports.length);  // one entry per volume
        List<BlockReportReplica> report = flatten(reports);
        assertEquals(2, report.size());
        for (BlockReportReplica r : report) {
            if (r.getBlock().getBlockId() == 1) {
                assertEquals(ReplicaState.FINALIZED, r.getState());
                assertEquals(10, r.getBlock().getNumBytes());
            } else {
                assertEquals(ReplicaState.RBW, r.getState());
            }
        }
        assertEquals(30, ds.getDfsUsed());
    }

    @Test
    void restartLoadsFinalizedAndTurnsRbwIntoRwr() throws IOException {
        writeFinalized(new Block(1, 0, 1001), data(100));
        writeRbw(new Block(2, 0, 1002), data(50));
        FsDatasetImpl restarted = newDataset(1);
        assertEquals(ReplicaState.FINALIZED, restarted.getReplica(1).getState());
        assertEquals(100, restarted.getReplica(1).getNumBytes());
        assertEquals(1001, restarted.getReplica(1).getGenerationStamp());
        assertEquals(ReplicaState.RWR, restarted.getReplica(2).getState());
        assertEquals(50, restarted.getReplica(2).getNumBytes());
    }

    // ------------------------------------------------------------ Volumes
    @Test
    void storageLocationParsesTypePrefixRelativeToHostRoot() {
        File root = new File("cluster/dn2");
        StorageLocation ssd = StorageLocation.parse("[SSD]/data/3/dfs/dn", root);
        assertEquals(StorageType.SSD, ssd.getStorageType());
        assertEquals(new File(root, "data/3/dfs/dn"), ssd.getDir());
        StorageLocation plain = StorageLocation.parse(" /data/1/dfs/dn ", root);
        assertEquals(StorageType.DISK, plain.getStorageType());
        assertEquals(new File(root, "data/1/dfs/dn"), plain.getDir());
    }

    @Test
    void blockIdDeterminesSubdirectories() {
        File root = new File("finalized");
        long id = 1073741825L + 3 * 0x10000 + 5 * 0x100;  // bits 16-20 = 3, bits 8-12 = 5
        assertEquals(new File(root, "subdir3/subdir5"), FsVolumeImpl.idToBlockDir(root, id));
        assertEquals(new File(root, "subdir0/subdir0"), FsVolumeImpl.idToBlockDir(root, 1073741825L));
    }

    @Test
    void roundRobinSpreadsNewReplicasOverAllVolumes() throws IOException {
        Set<String> storages = new HashSet<>();
        List<String> order = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            String id = ds.createRbw(new Block(i, 0, 1001)).getVolume().getStorageID();
            storages.add(id);
            order.add(id);
        }
        assertEquals(2, storages.size());
        assertEquals(order.get(0), order.get(2));
        assertEquals(order.get(1), order.get(3));
    }

    @Test
    void volumesAreFormattedWithVersionFilesAndKeepTheirStorageId() throws IOException {
        List<String> ids = new ArrayList<>();
        for (FsVolumeImpl v : ds.getVolumes()) {
            ids.add(v.getStorageID());
            File volumeDir = v.getStorageLocation().getDir();
            Properties p = FsVolumeImpl.loadVersion(volumeDir);
            assertEquals(v.getStorageID(), p.getProperty("storageID"));
            assertTrue(v.getStorageID().startsWith("DS-"));
            assertEquals(HdfsServerConstants.CLUSTER_ID, p.getProperty("clusterID"));
            assertEquals("uuid-test", p.getProperty("datanodeUuid"));
            assertTrue(new File(bpCurrent(volumeDir), "VERSION").isFile());
        }
        assertEquals(2, new HashSet<>(ids).size());
        List<String> afterRestart = newDataset(1).getVolumes().stream().map(FsVolumeImpl::getStorageID).toList();
        assertEquals(ids, afterRestart);
    }

    @Test
    void failedVolumeIsRemovedTogetherWithItsReplicas() throws IOException {
        ReplicaInfo onFirst = writeFinalized(new Block(1, 0, 1001), data(10));
        ReplicaInfo onSecond = writeFinalized(new Block(2, 0, 1001), data(20));
        assertTrue(onFirst.getVolume() != onSecond.getVolume());

        MiniDFSCluster.deleteRecursively(onSecond.getVolume().getStorageLocation().getDir());  // disk "broken"
        List<FsVolumeImpl> failed = ds.checkDataDir();
        assertEquals(List.of(onSecond.getVolume()), failed);
        assertNull(ds.getReplica(2));
        assertEquals(onFirst, ds.getReplica(1));
        assertEquals(1, ds.getNumFailedVolumes());
        assertTrue(ds.hasEnoughResource());  // 1 failure tolerated
        assertEquals(1, ds.getBlockReport().length);
        StorageReport[] reports = ds.getStorageReports();
        assertEquals(1, reports.length);
        assertEquals(onFirst.getVolume().getStorageID(), reports[0].getStorage().getStorageID());
        assertEquals(10, reports[0].getDfsUsed());
        // new replicas now land only on the remaining volume
        assertEquals(onFirst.getVolume(), ds.createRbw(new Block(3, 0, 1001)).getVolume());
        assertTrue(ds.checkDataDir().isEmpty());

        MiniDFSCluster.deleteRecursively(onFirst.getVolume().getStorageLocation().getDir());
        ds.checkDataDir();
        assertFalse(ds.hasEnoughResource());  // now the DataNode must shut down
    }

    @Test
    void volumeThatCannotBeUsedAtStartupCountsAsFailed() throws IOException {
        locations = StorageLocation.parseAll(List.of("/disk/1", "/disk/2"), tmp);
        File blocked = locations.get(1).getDir();
        assertTrue(blocked.getParentFile().mkdirs());
        assertTrue(blocked.createNewFile());  // a file instead of a directory
        FsDatasetImpl tolerant = newDataset(1);
        assertEquals(1, tolerant.getVolumes().size());
        assertEquals(1, tolerant.getNumFailedVolumes());
        IOException e = assertThrows(IOException.class, () -> newDataset(0));
        assertTrue(e.getMessage().contains("Too many failed volumes"), e.getMessage());
    }

    @Test
    void toleratedFailuresMustBeLessThanConfiguredVolumes() {
        assertThrows(IOException.class, () -> newDataset(2));
        assertThrows(IOException.class, () -> newDataset(-1));
    }

    @Test
    void sameBlockOnTwoVolumesKeepsNewerGenerationStamp() throws IOException {
        ReplicaInfo r = writeFinalized(new Block(9, 0, 1001), data(10));
        // place a copy with a higher GS on the other volume (e.g. after copying it over manually)
        File other = locations.get(0).getDir().equals(r.getVolume().getStorageLocation().getDir())
                ? locations.get(1).getDir() : locations.get(0).getDir();
        File dir = FsVolumeImpl.idToBlockDir(new File(bpCurrent(other), "finalized"), 9);
        assertTrue(dir.mkdirs());
        Files.copy(ds.getBlockFile(r).toPath(), new File(dir, "blk_9").toPath());
        Files.copy(ds.getMetaFile(r).toPath(), new File(dir, "blk_9_1002.meta").toPath());
        ReplicaInfo loaded = newDataset(1).getReplica(9);
        assertEquals(1002, loaded.getGenerationStamp());
        assertEquals(other, loaded.getVolume().getStorageLocation().getDir());
    }

    // ------------------------------------------------------------ deletion
    @Test
    void invalidateRequiresMatchingGenerationStamp() throws IOException {
        ReplicaInfo r = writeFinalized(new Block(1, 0, 1005), data(10));
        File blockFile = ds.getBlockFile(r);
        File metaFile = ds.getMetaFile(r);
        assertTrue(ds.invalidate(new Block[]{new Block(1, 0, 1001)}).isEmpty());  // old GS: do not delete
        assertEquals(ReplicaState.FINALIZED, ds.getReplica(1).getState());
        List<ReplicaInfo> deleted = ds.invalidate(new Block[]{new Block(1, 0, 1005), new Block(99, 0, 1)});
        assertEquals(List.of(new Block(1, 0, 0)), deleted.stream().map(ReplicaInfo::toBlock).toList());
        assertNull(ds.getReplica(1));
        assertFalse(blockFile.exists());
        assertFalse(metaFile.exists());
    }

    // -------------------------------------------------------- block recovery
    @Test
    void recoveryTruncatesRenamesAndFinalizes() throws IOException {
        byte[] d = data(2000);
        Block b = new Block(5, 0, 1001);
        writeRbw(b, d);

        ReplicaRecoveryInfo info = ds.initReplicaRecovery(recovering(b, 1010));
        assertEquals(2000, info.getNumBytes());
        assertEquals(ReplicaState.RBW, info.getOriginalReplicaState());
        assertEquals(ReplicaState.RUR, ds.getReplica(5).getState());

        Block result = ds.updateReplicaUnderRecovery(b, 1010, 1300).toBlock();
        assertEquals(1300, result.getNumBytes());
        assertEquals(1010, result.getGenerationStamp());
        ReplicaInfo r = ds.getReplica(5);
        assertEquals(ReplicaState.FINALIZED, r.getState());
        assertEquals(new File(bpCurrent(r), "finalized/subdir0/subdir0/blk_5_1010.meta"), ds.getMetaFile(r));

        // data truncated, CRCs match (also for the new partial chunk 1024..1300)
        byte[] onDisk = Files.readAllBytes(ds.getBlockFile(r).toPath());
        assertArrayEquals(Arrays.copyOf(d, 1300), onDisk);
        int[] expected = DataChecksum.computeChunkedSums(onDisk, 0, onDisk.length);
        try (DataInputStream in = new DataInputStream(new FileInputStream(ds.getMetaFile(r)))) {
            in.skipNBytes(FsDatasetImpl.META_HEADER_SIZE);
            for (int c : expected) {
                assertEquals(c, in.readInt());
            }
            assertEquals(-1, in.read());
        }
    }

    @Test
    void truncateToZero() throws IOException {
        Block b = new Block(5, 0, 1001);
        writeRbw(b, data(700));
        ds.initReplicaRecovery(recovering(b, 1002));
        ds.updateReplicaUnderRecovery(b, 1002, 0);
        ReplicaInfo r = ds.getReplica(5);
        assertEquals(0, ds.getBlockFile(r).length());
        assertEquals(FsDatasetImpl.META_HEADER_SIZE, ds.getMetaFile(r).length());
    }

    @Test
    void recoveryRejectsStaleOrOutdatedIds() throws IOException {
        Block b = new Block(5, 0, 1005);
        writeRbw(b, data(10));
        // NameNode knows a newer GS than the replica -> replica is stale
        assertThrows(IOException.class, () -> ds.initReplicaRecovery(recovering(new Block(5, 0, 1006), 1010)));
        // recoveryId must be greater than the replica's GS
        assertThrows(IOException.class, () -> ds.initReplicaRecovery(recovering(b, 1005)));
        ds.initReplicaRecovery(recovering(b, 1010));
        // an older recovery must not overtake a newer one
        assertThrows(IOException.class, () -> ds.initReplicaRecovery(recovering(b, 1008)));
        // update only with the current recoveryId
        assertThrows(IOException.class, () -> ds.updateReplicaUnderRecovery(b, 1009, 5));
        // do not extend
        assertThrows(IOException.class, () -> ds.updateReplicaUnderRecovery(b, 1010, 11));
    }

    @Test
    void recoveryOfMissingReplicaReturnsNull() throws IOException {
        assertNull(ds.initReplicaRecovery(recovering(new Block(77, 0, 1001), 1002)));
    }

    @Test
    void replicaUnderRecoveryCannotBeFinalizedByOldWriter() throws IOException {
        Block b = new Block(5, 0, 1001);
        ReplicaInfo r = writeRbw(b, data(10));
        ds.initReplicaRecovery(recovering(b, 1002));
        assertThrows(IOException.class, () -> ds.finalizeBlock(r));
    }
}
