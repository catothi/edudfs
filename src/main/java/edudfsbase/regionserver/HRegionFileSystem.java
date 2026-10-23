package edudfsbase.regionserver;

import edudfsbase.HConstants;
import edudfsbase.RegionInfo;
import edudfsbase.fs.CommonFSUtils;
import edudfsbase.fs.HFileSystem;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.HRegionFileSystem: the directory of a region
 * in HDFS and all file operations on it.
 * <pre>
 *   /hbase/data/default/&lt;table&gt;/&lt;encoded region&gt;/
 *       .regioninfo
 *       &lt;family&gt;/&lt;hfile&gt;
 *       .tmp/&lt;family&gt;/&lt;hfile&gt;      new HFiles are written here first ...
 *       recovered.edits/&lt;seqid&gt;
 * </pre>
 * ... and only then moved into the family directory with ONE rename (commitStoreFile).
 * A rename in HDFS is atomic (a pure NameNode operation) - readers therefore see either no file
 * or the complete one, never a half-written HFile.
 */
public class HRegionFileSystem {

    private final HFileSystem fs;
    private final RegionInfo regionInfo;
    private final String regionDir;

    private HRegionFileSystem(HFileSystem fs, RegionInfo regionInfo) {
        this.fs = fs;
        this.regionInfo = regionInfo;
        this.regionDir = CommonFSUtils.getRegionDir(regionInfo);
    }

    /** New region (create table): create the directory and write .regioninfo. */
    public static HRegionFileSystem createRegionOnFileSystem(HFileSystem fs, RegionInfo regionInfo) throws IOException {
        HRegionFileSystem rfs = new HRegionFileSystem(fs, regionInfo);
        fs.mkdirs(rfs.regionDir);
        fs.writeFile(rfs.regionDir + "/" + HConstants.REGIONINFO_FILE, regionInfo.toFileContent());
        return rfs;
    }

    public static HRegionFileSystem openRegionFromFileSystem(HFileSystem fs, RegionInfo regionInfo) throws IOException {
        HRegionFileSystem rfs = new HRegionFileSystem(fs, regionInfo);
        if (!fs.exists(rfs.regionDir)) {
            throw new IOException("Region directory does not exist: " + rfs.regionDir);
        }
        return rfs;
    }

    public HFileSystem getFileSystem() {
        return fs;
    }

    public String getRegionDir() {
        return regionDir;
    }

    public String getStoreDir(String family) {
        return regionDir + "/" + family;
    }

    public String getTempDir() {
        return regionDir + "/" + HConstants.HBASE_TEMP_DIRECTORY;
    }

    /** Name of a new HFile: random UUID without dashes, as in the original. */
    public static String createTempName() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public String createTempPath(String family) {
        return getTempDir() + "/" + family + "/" + createTempName();
    }

    /** Paths of all HFiles of a store. */
    public List<String> getStoreFiles(String family) throws IOException {
        List<String> r = new ArrayList<>();
        for (HdfsFileStatus st : fs.listStatus(getStoreDir(family))) {
            if (!st.isDir() && !st.isUnderConstruction()) {
                r.add(st.getPath());
            }
        }
        return r;
    }

    /** .tmp/&lt;family&gt;/&lt;name&gt; -&gt; &lt;family&gt;/&lt;name&gt; (atomic rename). */
    public String commitStoreFile(String family, String tmpPath) throws IOException {
        fs.mkdirs(getStoreDir(family));
        String dst = getStoreDir(family) + "/" + CommonFSUtils.getName(tmpPath);
        if (!fs.rename(tmpPath, dst)) {
            throw new IOException("Failed rename of " + tmpPath + " to " + dst);
        }
        return dst;
    }

    /**
     * Corresponds to HFileArchiver.archiveStoreFiles: HFiles replaced by a compaction are not
     * deleted but moved to /hbase/archive (snapshots or backups might still need them; in the
     * original the HFileCleaner deletes them later).
     */
    public void removeStoreFiles(String family, List<String> paths) throws IOException {
        String archiveDir = CommonFSUtils.getStoreArchivePath(regionInfo, family);
        fs.mkdirs(archiveDir);
        for (String p : paths) {
            if (!fs.rename(p, archiveDir + "/" + CommonFSUtils.getName(p))) {
                Log.warn("HFileArchiver", "Failed to archive " + p + ", deleting it");
                fs.delete(p, false);
            }
        }
    }

    public String getRecoveredEditsDir() {
        return regionDir + "/" + HConstants.RECOVERED_EDITS_DIR;
    }

    /** recovered.edits files sorted by name (= highest sequence ID contained, zero-padded). */
    public List<String> getRecoveredEditsFiles() throws IOException {
        List<String> r = new ArrayList<>();
        for (HdfsFileStatus st : fs.listStatus(getRecoveredEditsDir())) {
            if (!st.isDir() && !st.getPath().endsWith(".temp")) {
                r.add(st.getPath());
            }
        }
        r.sort(null);
        return r;
    }
}
