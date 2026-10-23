package edudfsbase;

import edudfsbase.util.Bytes;

/** Corresponds to org.apache.hadoop.hbase.HConstants (excerpt). */
public final class HConstants {

    private HConstants() {
    }

    /** "Now" - replaced by the RegionServer's current time on write. */
    public static final long LATEST_TIMESTAMP = Long.MAX_VALUE;
    public static final long OLDEST_TIMESTAMP = Long.MIN_VALUE;
    public static final long FOREVER = Integer.MAX_VALUE;
    public static final long NO_SEQNUM = -1;

    public static final byte[] EMPTY_START_ROW = Bytes.EMPTY_BYTE_ARRAY;
    public static final byte[] EMPTY_END_ROW = Bytes.EMPTY_BYTE_ARRAY;

    // ------------------------------------------------- directory layout in HDFS
    /** /hbase/data/&lt;namespace&gt;/&lt;table&gt;/... */
    public static final String BASE_NAMESPACE_DIR = "data";
    /** /hbase/WALs/&lt;servername&gt;/... */
    public static final String HREGION_LOGDIR_NAME = "WALs";
    /** /hbase/oldWALs: WALs whose edits are all flushed. */
    public static final String HREGION_OLDLOGDIR_NAME = "oldWALs";
    /** /hbase/archive: HFiles replaced by a compaction (original: deleted later by the HFileCleaner). */
    public static final String HFILE_ARCHIVE_DIRECTORY = "archive";
    public static final String HBASE_TEMP_DIRECTORY = ".tmp";
    public static final String SPLITTING_EXT = "-splitting";
    public static final String RECOVERED_EDITS_DIR = "recovered.edits";
    public static final String REGIONINFO_FILE = ".regioninfo";
    public static final String TABLEINFO_DIR = ".tabledesc";
    public static final String HBASE_ID_FILE_NAME = "hbase.id";
    public static final String VERSION_FILE_NAME = "hbase.version";
    /** Version of the file system layout (original: "8"). */
    public static final String FILE_SYSTEM_VERSION = "8";

    // ----------------------------------------------------------------- ports
    public static final int DEFAULT_MASTER_PORT = 16000;
    public static final int DEFAULT_REGIONSERVER_PORT = 16020;

    /** Pause factors for client retries (original HConstants.RETRY_BACKOFF), times hbase.client.pause. */
    public static final int[] RETRY_BACKOFF = {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};
}
