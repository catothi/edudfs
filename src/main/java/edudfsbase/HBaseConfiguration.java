package edudfsbase;

import edudfs.common.HdfsConfig;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hbase.HBaseConfiguration: reads conf/hbase-site.xml (from the
 * same configuration directory as core-site.xml and hdfs-site.xml).
 *
 * Precedence: -Dkey=value &gt; hbase-site.xml &gt; built-in default. The keys are the original ones;
 * sizes and times are much smaller than in the original, so that flushes, compactions, WAL rolls
 * and server failures can be observed with a few rows and within seconds.
 * As in the original, all times are in milliseconds.
 */
public final class HBaseConfiguration {

    private HBaseConfiguration() {
    }

    private static final Map<String, String> CONF = new HashMap<>();
    public static final File SITE_FILE = new File(HdfsConfig.CONF_DIR, "hbase-site.xml");

    static {
        if (SITE_FILE.isFile()) {
            CONF.putAll(HdfsConfig.parseXml(SITE_FILE));
        }
    }

    public static String get(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) {
            v = CONF.get(key);
        }
        return v == null ? def : v.trim();
    }

    public static long getLong(String key, long def) {
        String v = get(key, null);
        if (v == null) {
            return def;
        }
        String s = v.toLowerCase();
        int shift = "kmg".indexOf(s.charAt(s.length() - 1));
        return shift < 0 ? Long.parseLong(s) : Long.parseLong(s.substring(0, s.length() - 1)) << (10 * (shift + 1));
    }

    public static int getInt(String key, int def) {
        return (int) getLong(key, def);
    }

    public static double getDouble(String key, double def) {
        String v = get(key, null);
        return v == null ? def : Double.parseDouble(v);
    }

    // ---------------------------------------------------------- file system
    /**
     * hbase.rootdir: where HBase keeps everything in HDFS (original e.g. hdfs://nn:8020/hbase).
     * EduDFSBase uses the path part; the NameNode is the one from core-site.xml.
     */
    public static final String ROOT_DIR = rootDir();

    private static String rootDir() {
        String v = get("hbase.rootdir", "hdfs://localhost:9000/hbase");
        String p = v.contains("://") ? URI.create(v).getPath() : v;
        return p.endsWith("/") && p.length() > 1 ? p.substring(0, p.length() - 1) : p;
    }

    // --------------------------------------------------------------- network
    public static final String HOSTNAME = "localhost";
    /** hbase.master.port (original: 16000). */
    public static final int MASTER_PORT = getInt("hbase.master.port", HConstants.DEFAULT_MASTER_PORT);
    /** hbase.regionserver.port (original: 16020). RegionServer i of the minicluster uses port + i. */
    public static final int REGIONSERVER_PORT = getInt("hbase.regionserver.port", HConstants.DEFAULT_REGIONSERVER_PORT);

    // -------------------------------------------------------------- MemStore
    /** hbase.hregion.memstore.flush.size (original: 128 MB). */
    public static final long MEMSTORE_FLUSH_SIZE = getLong("hbase.hregion.memstore.flush.size", 64 * 1024);

    // ------------------------------------------------------------ compaction
    /** hbase.hstore.compactionThreshold: minor compaction from this many HFiles per store (original: 3). */
    public static final int COMPACTION_MIN_FILES = getInt("hbase.hstore.compactionThreshold", 3);
    /** hbase.hstore.compaction.max: at most this many files per minor compaction (original: 10). */
    public static final int COMPACTION_MAX_FILES = getInt("hbase.hstore.compaction.max", 10);
    /** hbase.hstore.compaction.ratio (original: 1.2). */
    public static final double COMPACTION_RATIO = getDouble("hbase.hstore.compaction.ratio", 1.2);
    /** hbase.hstore.compaction.min.size: smaller files are always selected (original: = flush size). */
    public static final long COMPACTION_MIN_SIZE = getLong("hbase.hstore.compaction.min.size", MEMSTORE_FLUSH_SIZE);
    /** hbase.hregion.majorcompaction: interval of the periodic major compaction, 0 = off (original: 7 days). */
    public static final long MAJOR_COMPACTION_PERIOD = getLong("hbase.hregion.majorcompaction", 0);
    /** hbase.server.thread.wakefrequency: interval of the background chores (original: 10 s). */
    public static final long THREAD_WAKE_FREQUENCY = getLong("hbase.server.thread.wakefrequency", 1000);

    // -------------------------------------------------------------------- WAL
    /** hbase.regionserver.hlog.blocksize (original: 2 x HDFS block size). */
    public static final long WAL_BLOCK_SIZE = getLong("hbase.regionserver.hlog.blocksize", 64 * 1024);
    /** hbase.regionserver.logroll.multiplier: roll the WAL at blocksize x multiplier (original: 0.5). */
    public static final double LOGROLL_MULTIPLIER = getDouble("hbase.regionserver.logroll.multiplier", 0.5);
    /** hbase.regionserver.logroll.period: roll at least this often (original: 1 h). */
    public static final long LOGROLL_PERIOD = getLong("hbase.regionserver.logroll.period", 3600_000);

    // ------------------------------------------------------- read path
    /** io.storefile.bloom.error.rate: target false positive rate of the Bloom filters (original: 1 %). */
    public static final double BLOOM_ERROR_RATE = getDouble("io.storefile.bloom.error.rate", 0.01);
    /** hfile.block.cache.size: share of the heap for the block cache (original: 0.4). */
    public static final double BLOCK_CACHE_SIZE = getDouble("hfile.block.cache.size", 0.4);

    // ------------------------------------------------- master / RegionServers
    /** hbase.regionserver.msginterval: heartbeat RegionServer -&gt; master (original: 3 s). */
    public static final long MSG_INTERVAL = getLong("hbase.regionserver.msginterval", 1000);
    /**
     * zookeeper.session.timeout (original: 90 s). In the original a RegionServer counts as dead when its
     * ZooKeeper session expires. EduDFSBase has no ZooKeeper: the master declares a RegionServer dead
     * when it has not received a heartbeat for this long.
     */
    public static final long SESSION_TIMEOUT = getLong("zookeeper.session.timeout", 6000);
    /** hbase.master.wait.on.regionservers.mintostart (original: 1). */
    public static final int WAIT_ON_RS_MIN = getInt("hbase.master.wait.on.regionservers.mintostart", 1);
    /** hbase.master.wait.on.regionservers.timeout (original: 4.5 s). */
    public static final long WAIT_ON_RS_TIMEOUT = getLong("hbase.master.wait.on.regionservers.timeout", 3000);

    // ------------------------------------------------------------------ client
    /** hbase.client.retries.number (original: 15). */
    public static final int CLIENT_RETRIES = getInt("hbase.client.retries.number", 15);
    /** hbase.client.pause: base of the retry pause, times HConstants.RETRY_BACKOFF (original: 100 ms). */
    public static final long CLIENT_PAUSE = getLong("hbase.client.pause", 100);
    /** hbase.client.scanner.caching: rows per next() RPC of a scan (original: unlimited, limited by size). */
    public static final int SCANNER_CACHING = getInt("hbase.client.scanner.caching", 100);
    /** hbase.client.scanner.timeout.period: an unused scanner on the RegionServer expires (original: 60 s). */
    public static final long SCANNER_TIMEOUT = getLong("hbase.client.scanner.timeout.period", 60_000);
}
