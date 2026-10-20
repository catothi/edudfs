package edudfs.tools;

import edudfs.client.DFSClient;
import edudfs.client.DFSInputStream;
import edudfs.client.DFSOutputStream;
import edudfs.common.HdfsConfig;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LocatedBlock;
import edudfs.server.datanode.DataNode;
import edudfs.server.datanode.FsVolumeImpl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

/**
 * Guided tour of the core concepts. Starts a MiniDFSCluster (1 NameNode, 4 DataNodes)
 * and runs through several scenarios one after another. The log output of the NameNode, DataNodes
 * and client shows what happens internally.
 */
public class Demo {

    private static final int BLOCK_SIZE = 128 * 1024;  // small blocks so that files have several
    private static final short REPL = 3;

    public static void main(String[] args) throws Exception {
        File base = new File("demo-data");
        MiniDFSCluster.deleteRecursively(base);
        try (MiniDFSCluster cluster = new MiniDFSCluster(4, base)) {
            cluster.waitActive();
            DFSClient client = new DFSClient();

            // ------------------------------------------------------------------
            banner("1) Namespace, INode IDs and sequential block IDs",
                    "We write 300 KB with a 128 KB block size -> 3 blocks. Watch for:",
                    " - INode IDs from 16386 (16385 = root), block IDs from 1073741825 (2^30 + 1),",
                    " - generation stamps from 1001, pipeline of 3 DataNodes, commit -> COMPLETE.");
            client.mkdirs("/user/student");
            byte[] data = randomBytes(300 * 1024);
            try (DFSOutputStream out = client.create("/user/student/data.bin", false, REPL, BLOCK_SIZE)) {
                out.write(data);
            }
            metasave(client);

            // ------------------------------------------------------------------
            banner("2) Reading: client gets block locations from the NameNode, data directly from the DataNodes");
            check("Content identical", Arrays.equals(data, readAll(client, "/user/student/data.bin")));

            // ------------------------------------------------------------------
            banner("3) Rename is a pure metadata operation (INode is moved, ID stays the same)");
            client.rename("/user/student/data.bin", "/user/student/renamed.bin");
            HdfsFileStatus st = client.getFileInfo("/user/student/renamed.bin");
            System.out.println("   /user/student/renamed.bin -> inode " + st.getFileId() + ", " + st.getLen() + " bytes");

            // ------------------------------------------------------------------
            banner("4) Corrupt replica: checksums protect against bit rot",
                    "We flip a byte in the block file of the FIRST replica of block 0.",
                    "The client detects the CRC error, reads from the next DataNode and reports the",
                    "replica (reportBadBlocks). The NameNode deletes it and has it re-replicated.");
            LocatedBlock lb0 = client.getNamenode().getBlockLocations("/user/student/renamed.bin", 0, 1)
                    .getLocatedBlocks().get(0);
            DataNode victim = cluster.getDataNodeByXferPort(lb0.getLocations()[0].getXferPort());
            corruptByte(victim.getBlockFile(lb0.getBlock().getBlockId()), 1000);
            System.out.println("   Byte 1000 of " + lb0.getBlock() + " on " + victim + " modified");
            check("Content still correct", Arrays.equals(data, readAll(client, "/user/student/renamed.bin")));
            sleep(6000, "waiting for invalidate + re-replication");
            metasave(client);

            // ------------------------------------------------------------------
            banner("5) DataNode failure -> re-replication",
                    "We stop the DataNode on port 50010. After " + HdfsConfig.HEARTBEAT_EXPIRE_MS / 1000
                            + " s without a heartbeat the NameNode declares it dead;",
                    "its replicas are missing, the RedundancyMonitor sends commands to other DataNodes (DNA_TRANSFER).");
            cluster.stopDataNode(0);
            sleep(HdfsConfig.HEARTBEAT_EXPIRE_MS + 6000, "waiting for dead detection + re-replication");
            metasave(client);
            check("Content correct after re-replication",
                    Arrays.equals(data, readAll(client, "/user/student/renamed.bin")));

            // ------------------------------------------------------------------
            volumeFailure(cluster, client, data);

            // ------------------------------------------------------------------
            banner("7) Lease: soft limit (" + HdfsConfig.LEASE_SOFTLIMIT_MS / 1000 + " s)",
                    "Client A writes 200 KB and 'crashes' (no close, no lease renewal).",
                    "Block 1 (128 KB) is complete; of block 2 only 1 packet (64 KB) has been sent,",
                    "the remaining 8 KB were still in the client buffer and are lost.");
            DFSClient clientA = new DFSClient();
            DFSOutputStream outA = clientA.create("/user/student/soft.bin", false, REPL, BLOCK_SIZE);
            outA.write(randomBytes(200 * 1024));
            sleep(1000, "packets in flight");
            clientA.abort();
            metasave(client);

            DFSClient clientB = new DFSClient();
            tryCreate(clientB, "/user/student/soft.bin", "Client B before the soft limit expires");
            sleep(HdfsConfig.LEASE_SOFTLIMIT_MS + 500, "letting the soft limit expire");
            tryCreate(clientB, "/user/student/soft.bin", "Client B after the soft limit expired (starts recovery)");
            waitUntilClosed(client, "/user/student/soft.bin");
            metasave(client);
            tryCreate(clientB, "/user/student/soft.bin", "Client B after the recovery");

            // ------------------------------------------------------------------
            banner("8) Lease: hard limit (" + HdfsConfig.LEASE_HARDLIMIT_MS / 1000 + " s)",
                    "Client C writes 100 KB and crashes. Nobody else is interested in the file.",
                    "Once the hard limit expires, the LeaseManager monitor starts the recovery itself:",
                    "lease -> HDFS_NameNode, new GS (recoveryId), the primary DataNode synchronizes the",
                    "replicas (min. length) and reports commitBlockSynchronization.");
            DFSClient clientC = new DFSClient();
            DFSOutputStream outC = clientC.create("/user/student/hard.bin", false, REPL, BLOCK_SIZE);
            outC.write(randomBytes(100 * 1024));
            sleep(1000, "packets in flight");
            clientC.abort();
            waitUntilClosed(client, "/user/student/hard.bin");
            metasave(client);

            // ------------------------------------------------------------------
            banner("9) Deletion: blocks move to the invalidate queues and are deleted via heartbeat");
            client.delete("/user/student", true);
            sleep(3000, "waiting for heartbeats");
            metasave(client);

            banner("Done.");
            clientB.close();
            client.close();
        }
        System.exit(0);
    }

    // ------------------------------------------------------------------- Scenario 6
    private static void volumeFailure(MiniDFSCluster cluster, DFSClient client, byte[] data) throws Exception {
        banner("6) Disk failure: ONE volume of a DataNode fails",
                "Each DataNode has " + HdfsConfig.DATANODE_DATA_DIRS.size() + " volumes (dfs.datanode.data.dir = "
                        + HdfsConfig.DATANODE_DATA_DIRS + ").",
                "We delete the volume holding a replica of block 0. The DataNode notices this on the",
                "next heartbeat and reports one storage fewer + 1 failed volume. The NameNode sets",
                "the storage to FAILED, removes ONLY its replicas and has them re-replicated.",
                "The DataNode stays alive (dfs.datanode.failed.volumes.tolerated = "
                        + HdfsConfig.FAILED_VOLUMES_TOLERATED + ").");
        if (HdfsConfig.DATANODE_DATA_DIRS.size() < 2 || HdfsConfig.FAILED_VOLUMES_TOLERATED < 1) {
            System.out.println("   (skipped: requires >= 2 volumes and failed.volumes.tolerated >= 1)");
            return;
        }
        LocatedBlock lb = client.getNamenode().getBlockLocations("/user/student/renamed.bin", 0, 1)
                .getLocatedBlocks().get(0);
        DataNode victim = cluster.getDataNodeByXferPort(lb.getLocations()[0].getXferPort());
        FsVolumeImpl volume = victim.getFSDataset().getReplica(lb.getBlock().getBlockId()).getVolume();
        File dir = volume.getStorageLocation().getDir();
        System.out.println("   " + victim + ": deleting volume " + volume.getStorageID() + " = " + dir);
        MiniDFSCluster.deleteRecursively(dir);
        sleep(HdfsConfig.HEARTBEAT_INTERVAL_MS + 6000, "waiting for detection (heartbeat) + re-replication");
        metasave(client);
        check("Content correct after disk failure", Arrays.equals(data, readAll(client, "/user/student/renamed.bin")));
    }

    // ------------------------------------------------------------------- Helpers
    private static void banner(String title, String... lines) {
        System.out.println();
        System.out.println("#".repeat(100));
        System.out.println("# " + title);
        for (String l : lines) {
            System.out.println("#   " + l);
        }
        System.out.println("#".repeat(100));
    }

    private static void metasave(DFSClient client) throws IOException {
        System.out.println("\n----- metasave -----------------------------------------------------------------");
        System.out.print(client.metaSave());
        System.out.println("--------------------------------------------------------------------------------\n");
    }

    private static void check(String what, boolean ok) {
        System.out.println("   >>> " + what + ": " + (ok ? "OK" : "ERROR!"));
    }

    private static void tryCreate(DFSClient c, String path, String who) {
        try {
            c.create(path, false, REPL, BLOCK_SIZE).close();
            System.out.println("   >>> " + who + ": create succeeded");
        } catch (IOException e) {
            System.out.println("   >>> " + who + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static void waitUntilClosed(DFSClient c, String path) throws Exception {
        System.out.println("   ... waiting until " + path + " is closed");
        long start = System.currentTimeMillis();
        while (true) {
            HdfsFileStatus s = c.getFileInfo(path);
            if (s != null && !s.isUnderConstruction()) {
                System.out.printf("   >>> %s closed after %.1f s, length %d bytes%n", path,
                        (System.currentTimeMillis() - start) / 1000.0, s.getLen());
                return;
            }
            Thread.sleep(250);
        }
    }

    private static byte[] readAll(DFSClient c, String path) throws IOException {
        try (DFSInputStream in = c.open(path)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            in.transferTo(bos);
            return bos.toByteArray();
        }
    }

    private static void corruptByte(File f, long pos) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(pos);
            int b = raf.read();
            raf.seek(pos);
            raf.write(b ^ 0xFF);
        }
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        new Random(n).nextBytes(b);
        return b;
    }

    private static void sleep(long ms, String why) throws InterruptedException {
        System.out.println("   ... " + why + " (" + ms / 1000.0 + " s)");
        Thread.sleep(ms);
    }
}
