package edubase.master;

import edubase.HConstants;
import edubase.RegionInfo;
import edubase.client.TableDescriptor;
import edubase.fs.CommonFSUtils;
import edubase.fs.HFileSystem;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Corresponds to org.apache.hadoop.hbase.master.MasterFileSystem: creates the directory layout under
 * hbase.rootdir and finds tables and regions in it.
 *
 * Simplification: the original learns the regions from hbase:meta. EduBase keeps hbase:meta only in
 * the master's memory; after a master restart it rebuilds the content from the .tableinfo and
 * .regioninfo files (comparable to the repair tool HBCK's "OfflineMetaRepair").
 */
public class MasterFileSystem {

    private static final String LOG = "MasterFileSystem";

    private final HFileSystem fs;

    MasterFileSystem(HFileSystem fs) throws IOException {
        this.fs = fs;
        checkRootDir();
    }

    private void checkRootDir() throws IOException {
        String root = CommonFSUtils.getRootDir();
        for (String d : new String[]{root, CommonFSUtils.getNamespaceDir(), CommonFSUtils.getWALRootDir(),
                CommonFSUtils.getOldWALDir(), root + "/" + HConstants.HFILE_ARCHIVE_DIRECTORY}) {
            fs.mkdirs(d);
        }
        String version = root + "/" + HConstants.VERSION_FILE_NAME;
        if (!fs.exists(version)) {
            fs.writeFile(version, HConstants.FILE_SYSTEM_VERSION + "\n");
            fs.writeFile(root + "/" + HConstants.HBASE_ID_FILE_NAME, UUID.randomUUID() + "\n");
            Log.info(LOG, "Created " + root + " (hbase.version=" + HConstants.FILE_SYSTEM_VERSION + ")");
        }
    }

    HFileSystem getFileSystem() {
        return fs;
    }

    /** Tables with their regions, read from HDFS. */
    Map<TableDescriptor, List<RegionInfo>> loadTables() throws IOException {
        Map<TableDescriptor, List<RegionInfo>> r = new LinkedHashMap<>();
        for (HdfsFileStatus tableDir : fs.listStatus(CommonFSUtils.getNamespaceDir())) {
            String info = tableDir.getPath() + "/" + HConstants.TABLEINFO_DIR + "/.tableinfo.0000000001";
            if (!tableDir.isDir() || !fs.exists(info)) {
                continue;
            }
            TableDescriptor htd = TableDescriptor.parseFrom(fs.readFile(info));
            List<RegionInfo> regions = new ArrayList<>();
            for (HdfsFileStatus regionDir : fs.listStatus(tableDir.getPath())) {
                String ri = regionDir.getPath() + "/" + HConstants.REGIONINFO_FILE;
                if (regionDir.isDir() && fs.exists(ri)) {
                    regions.add(RegionInfo.parseFrom(fs.readFile(ri)));
                }
            }
            r.put(htd, regions);
        }
        return r;
    }

    void writeTableDescriptor(TableDescriptor htd) throws IOException {
        fs.writeFile(CommonFSUtils.getTableInfoPath(htd.getTableName()), htd.toFileContent());
    }

    void deleteTable(TableDescriptor htd) throws IOException {
        fs.delete(CommonFSUtils.getTableDir(htd.getTableName()), true);
    }
}
