package edudfs.server.datanode;

import edudfs.protocol.StorageType;
import edudfs.server.common.HdfsServerConstants;
import edudfs.server.protocol.DatanodeStorage;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

/**
 * Corresponds to FsVolumeImpl + BlockPoolSlice (+ the part of DataStorage / BlockPoolSliceStorage
 * that writes the VERSION files): ONE volume = one disk = one entry in dfs.datanode.data.dir.
 *
 * Layout (as in the original):
 * <pre>
 * &lt;volume&gt;/current/VERSION                                   storageID, clusterID, datanodeUuid
 * &lt;volume&gt;/current/&lt;BP-ID&gt;/current/VERSION                  namespaceID, blockpoolID
 * &lt;volume&gt;/current/&lt;BP-ID&gt;/current/finalized/subdir&lt;a&gt;/subdir&lt;b&gt;/blk_&lt;id&gt;
 *                                                          .../blk_&lt;id&gt;_&lt;gs&gt;.meta
 * &lt;volume&gt;/current/&lt;BP-ID&gt;/current/rbw/blk_&lt;id&gt;, blk_&lt;id&gt;_&lt;gs&gt;.meta
 * </pre>
 * Simplifications: no tmp/ (re-replication writes via rbw/ here), no in_use.lock,
 * no layout upgrades (previous/, finalized rollback), only one block pool.
 */
public class FsVolumeImpl {

    private final StorageLocation location;
    private final String storageID;
    /** &lt;volume&gt;/current/&lt;BP-ID&gt;/current */
    private final File bpCurrentDir;
    private final File finalizedDir;
    private final File rbwDir;

    /**
     * Opens a volume. If it is empty (new disk), it is "formatted": a new storageID,
     * VERSION files and block pool directories are created.
     */
    FsVolumeImpl(StorageLocation location, String datanodeUuid) throws IOException {
        this.location = location;
        File current = new File(location.getDir(), "current");
        this.bpCurrentDir = new File(new File(current, HdfsServerConstants.BLOCK_POOL_ID), "current");
        this.finalizedDir = new File(bpCurrentDir, "finalized");
        this.rbwDir = new File(bpCurrentDir, "rbw");
        if (!finalizedDir.isDirectory() && !finalizedDir.mkdirs() || !rbwDir.isDirectory() && !rbwDir.mkdirs()) {
            throw new IOException("Cannot create storage directories in " + location.getDir());
        }

        // <volume>/current/VERSION (original: DataStorage)
        Properties p = loadVersion(location.getDir());
        String id = p.getProperty("storageID");
        if (id == null) {
            id = DatanodeStorage.generateUuid();
        }
        String clusterId = p.getProperty("clusterID");
        if (clusterId != null && !clusterId.equals(HdfsServerConstants.CLUSTER_ID)) {
            throw new IOException("Incompatible clusterIDs in " + location.getDir() + ": namenode clusterID = "
                    + HdfsServerConstants.CLUSTER_ID + "; datanode clusterID = " + clusterId);
        }
        this.storageID = id;
        p.setProperty("storageID", storageID);
        p.setProperty("clusterID", HdfsServerConstants.CLUSTER_ID);
        p.setProperty("cTime", "0");
        p.setProperty("datanodeUuid", datanodeUuid);
        p.setProperty("storageType", "DATA_NODE");
        p.setProperty("layoutVersion", String.valueOf(HdfsServerConstants.DATANODE_LAYOUT_VERSION));
        storeVersion(new File(current, "VERSION"), p, "DataNode storage (volume)");

        // <volume>/current/<BP-ID>/current/VERSION (original: BlockPoolSliceStorage)
        Properties bp = new Properties();
        bp.setProperty("namespaceID", String.valueOf(HdfsServerConstants.NAMESPACE_ID));
        bp.setProperty("cTime", "0");
        bp.setProperty("blockpoolID", HdfsServerConstants.BLOCK_POOL_ID);
        bp.setProperty("layoutVersion", String.valueOf(HdfsServerConstants.DATANODE_LAYOUT_VERSION));
        storeVersion(new File(bpCurrentDir, "VERSION"), bp, "Block pool slice storage");
    }

    /** Reads &lt;volume&gt;/current/VERSION (empty if the file does not exist). */
    static Properties loadVersion(File volumeDir) throws IOException {
        Properties p = new Properties();
        File version = new File(new File(volumeDir, "current"), "VERSION");
        if (version.exists()) {
            try (Reader r = new FileReader(version)) {
                p.load(r);
            }
        }
        return p;
    }

    private static void storeVersion(File f, Properties p, String comment) throws IOException {
        try (Writer w = new FileWriter(f)) {
            p.store(w, "edudfs " + comment);
        }
    }

    /**
     * Like DatanodeUtil.idToBlockDir: two directory levels derived from bits of the block ID
     * (32 options each -> max. 1024 directories). This way there are never too many files
     * in one directory, and the location of a block does not need to be stored anywhere.
     */
    static File idToBlockDir(File root, long blockId) {
        int d1 = (int) ((blockId >> 16) & 0x1F);
        int d2 = (int) ((blockId >> 8) & 0x1F);
        return new File(root, "subdir" + d1 + File.separator + "subdir" + d2);
    }

    /** Target directory of a FINALIZED replica; created if needed. */
    File getFinalizedDir(long blockId) throws IOException {
        File dir = idToBlockDir(finalizedDir, blockId);
        if (!dir.isDirectory() && !dir.mkdirs()) {
            throw new IOException("Cannot create " + dir);
        }
        return dir;
    }

    File getFinalizedRoot() {
        return finalizedDir;
    }

    File getRbwDir() {
        return rbwDir;
    }

    /**
     * Essentially corresponds to FsVolumeImpl.check() / DiskChecker.checkDir: are the directories
     * still there and writable? (Original: additionally writes/reads probe files.)
     */
    boolean isHealthy() {
        return finalizedDir.isDirectory() && finalizedDir.canWrite() && rbwDir.isDirectory() && rbwDir.canWrite();
    }

    public String getStorageID() {
        return storageID;
    }

    StorageType getStorageType() {
        return location.getStorageType();
    }

    public StorageLocation getStorageLocation() {
        return location;
    }

    DatanodeStorage toDatanodeStorage() {
        return new DatanodeStorage(storageID, DatanodeStorage.State.NORMAL, getStorageType());
    }

    /** Size of the disk. Here all volumes reside on the same real disk. */
    long getCapacity() {
        return location.getDir().getTotalSpace();
    }

    /** Free space (original: minus dfs.datanode.du.reserved). */
    long getAvailable() {
        return location.getDir().getUsableSpace();
    }

    @Override
    public String toString() {
        return location + " (" + storageID + ")";
    }
}
