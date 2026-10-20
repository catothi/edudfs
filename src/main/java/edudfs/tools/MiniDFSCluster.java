package edudfs.tools;

import edudfs.common.HdfsConfig;
import edudfs.server.datanode.DataNode;
import edudfs.server.namenode.NameNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Corresponds (heavily simplified) to org.apache.hadoop.hdfs.MiniDFSCluster:
 * NameNode + n DataNodes in ONE JVM - they still communicate over real
 * RMI/TCP connections on localhost.
 *
 * Ports: NameNode 9000, DataNode i: xfer 50010+i, ipc 50020+i (Hadoop 2 classics).
 *
 * Each DataNode gets &lt;baseDir&gt;/dn&lt;i&gt; as the root of its "machine"; below it are the
 * volumes from dfs.datanode.data.dir (e.g. &lt;baseDir&gt;/dn0/data/1/dfs/dn and .../data/2/dfs/dn).
 * The original MiniDFSCluster instead overrides dfs.datanode.data.dir per DataNode
 * (data/data1, data/data2 for DN 0, data/data3, data/data4 for DN 1, ...).
 */
public class MiniDFSCluster implements Closeable {

    public static final int BASE_XFER_PORT = 50010;
    public static final int BASE_IPC_PORT = 50020;

    private final File baseDir;
    private NameNode namenode;
    private final List<DataNode> datanodes = new ArrayList<>();

    public MiniDFSCluster(int numDataNodes, File baseDir) throws IOException {
        this.baseDir = baseDir;
        namenode = new NameNode(HdfsConfig.NAMENODE_RPC_PORT);
        namenode.start();
        for (int i = 0; i < numDataNodes; i++) {
            datanodes.add(null);
            startDataNode(i);
        }
    }

    /** Restarts the NameNode. Without persistence the namespace is empty afterwards. */
    public void restartNameNode() throws IOException {
        namenode.stop();
        namenode = new NameNode(HdfsConfig.NAMENODE_RPC_PORT);
        namenode.start();
    }

    /** Root directory (simulated machine) of DataNode i. */
    public File getHostRoot(int i) {
        return new File(baseDir, "dn" + i);
    }

    public void startDataNode(int i) throws IOException {
        DataNode dn = new DataNode(getHostRoot(i), BASE_XFER_PORT + i, BASE_IPC_PORT + i,
                HdfsConfig.NAMENODE_HOST, HdfsConfig.NAMENODE_RPC_PORT);
        dn.start();
        datanodes.set(i, dn);
    }

    public void stopDataNode(int i) {
        DataNode dn = datanodes.get(i);
        if (dn != null) {
            dn.shutdown();
            datanodes.set(i, null);
        }
    }

    public DataNode getDataNode(int i) {
        return datanodes.get(i);
    }

    /** Finds a DataNode by its xferPort. */
    public DataNode getDataNodeByXferPort(int port) {
        int i = port - BASE_XFER_PORT;
        return i >= 0 && i < datanodes.size() ? datanodes.get(i) : null;
    }

    public NameNode getNameNode() {
        return namenode;
    }

    /** Waits until all started DataNodes are registered and have reported their storages (with free space). */
    public void waitActive() throws InterruptedException {
        long expected = datanodes.stream().filter(d -> d != null).count();
        while (true) {
            namenode.getNamesystem().readLock();
            long live;
            try {
                live = namenode.getNamesystem().getBlockManager().getDatanodeManager().getLiveDatanodes().stream()
                        .filter(dn -> dn.getRemaining() > 0).count();
            } finally {
                namenode.getNamesystem().readUnlock();
            }
            if (live >= expected) {
                return;
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < datanodes.size(); i++) {
            stopDataNode(i);
        }
        namenode.stop();
    }

    public static void deleteRecursively(File dir) throws IOException {
        if (!dir.exists()) {
            return;
        }
        try (Stream<java.nio.file.Path> walk = Files.walk(dir.toPath())) {
            walk.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }

    /** Starts a cluster and keeps it running - e.g. to work on it with DFSShell. */
    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        File base = new File(args.length > 1 ? args[1] : "data");
        MiniDFSCluster cluster = new MiniDFSCluster(n, base);
        cluster.waitActive();
        System.out.println("\nMiniDFSCluster running: NameNode localhost:" + HdfsConfig.NAMENODE_RPC_PORT + ", "
                + n + " DataNodes, data under " + base.getAbsolutePath() + "/dn<i>" + "\nConfiguration: "
                + HdfsConfig.LOADED_RESOURCES + ", volumes per DataNode: " + HdfsConfig.DATANODE_DATA_DIRS
                + "\nStop with Ctrl+C\n");
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::close));
        Thread.currentThread().join();
    }
}
