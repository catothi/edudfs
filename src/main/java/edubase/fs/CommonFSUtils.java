package edubase.fs;

import edubase.HBaseConfiguration;
import edubase.HConstants;
import edubase.RegionInfo;
import edubase.ServerName;
import edubase.TableName;

/**
 * Corresponds to org.apache.hadoop.hbase.util.CommonFSUtils (+ HFileArchiveUtil, AbstractFSWALProvider):
 * where what lies in HDFS. The layout is the original one:
 * <pre>
 * /hbase
 *   hbase.id, hbase.version
 *   data/default/&lt;table&gt;/
 *       .tabledesc/.tableinfo.0000000001          table descriptor
 *       &lt;encoded region&gt;/
 *           .regioninfo                           RegionInfo
 *           &lt;family&gt;/&lt;hfile&gt;                     the HFiles of a store
 *           .tmp/&lt;family&gt;/&lt;hfile&gt;                being written (flush, compaction)
 *           recovered.edits/&lt;seqid&gt;             WAL edits of a crashed RegionServer
 *   WALs/&lt;servername&gt;/&lt;servername&gt;.&lt;ts&gt;           the WALs of every RegionServer
 *   WALs/&lt;servername&gt;-splitting/                    while the master splits them
 *   oldWALs/                                      WALs whose edits are all flushed
 *   archive/data/default/&lt;table&gt;/&lt;region&gt;/&lt;family&gt;/   HFiles replaced by a compaction
 * </pre>
 */
public final class CommonFSUtils {

    private CommonFSUtils() {
    }

    public static String getRootDir() {
        return HBaseConfiguration.ROOT_DIR;
    }

    public static String getTableDir(TableName table) {
        return getRootDir() + "/" + HConstants.BASE_NAMESPACE_DIR + "/" + table.getNamespaceAsString() + "/"
                + table.getQualifierAsString();
    }

    public static String getNamespaceDir() {
        return getRootDir() + "/" + HConstants.BASE_NAMESPACE_DIR + "/" + TableName.DEFAULT_NAMESPACE_NAME;
    }

    public static String getTableInfoPath(TableName table) {
        return getTableDir(table) + "/" + HConstants.TABLEINFO_DIR + "/.tableinfo.0000000001";
    }

    public static String getRegionDir(RegionInfo region) {
        return getTableDir(region.getTable()) + "/" + region.getEncodedName();
    }

    public static String getWALRootDir() {
        return getRootDir() + "/" + HConstants.HREGION_LOGDIR_NAME;
    }

    /** /hbase/WALs/localhost,16020,1761213640123 */
    public static String getWALDirectoryName(ServerName sn) {
        return getWALRootDir() + "/" + sn;
    }

    /** File name prefix of a WAL: the server name with ',' encoded as %2C (as in the original). */
    public static String getWALPrefix(ServerName sn) {
        return sn.toString().replace(",", "%2C");
    }

    public static String getOldWALDir() {
        return getRootDir() + "/" + HConstants.HREGION_OLDLOGDIR_NAME;
    }

    /** /hbase/archive/data/default/&lt;table&gt;/&lt;region&gt;/&lt;family&gt; */
    public static String getStoreArchivePath(RegionInfo region, String family) {
        return getRootDir() + "/" + HConstants.HFILE_ARCHIVE_DIRECTORY + "/" + HConstants.BASE_NAMESPACE_DIR + "/"
                + region.getTable().getNamespaceAsString() + "/" + region.getTable().getQualifierAsString() + "/"
                + region.getEncodedName() + "/" + family;
    }

    public static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }
}
