package edudfs.common;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Central configuration (corresponds to org.apache.hadoop.conf.Configuration + DFSConfigKeys).
 *
 * As in the original, ALL processes (NameNode, DataNodes, client) read the same files
 * core-site.xml and hdfs-site.xml from the configuration directory (original: $HADOOP_CONF_DIR,
 * here: -Dedudfs.conf.dir, otherwise $HADOOP_CONF_DIR, otherwise ./conf).
 *
 * Precedence (who wins): -Dkey=value &gt; XML file &gt; built-in default.
 * In addition there are older short keys (edudfs.*.ms) that the tests set in pom.xml.
 *
 * The timings are greatly shortened compared to the original so that effects
 * such as lease recovery or re-replication can be observed in seconds instead of minutes.
 */
public final class HdfsConfig {

    private HdfsConfig() {
    }

    /** Directory containing core-site.xml and hdfs-site.xml. */
    public static final File CONF_DIR = confDir();
    private static final Map<String, String> CONF = new HashMap<>();
    /** Files actually loaded (for log output). */
    public static final List<File> LOADED_RESOURCES = new ArrayList<>();

    static {
        for (String name : new String[]{"core-site.xml", "hdfs-site.xml"}) {
            File f = new File(CONF_DIR, name);
            if (f.isFile()) {
                CONF.putAll(parseXml(f));
                LOADED_RESOURCES.add(f);
            }
        }
    }

    private static File confDir() {
        String dir = System.getProperty("edudfs.conf.dir");
        if (dir == null) {
            dir = System.getenv("HADOOP_CONF_DIR");
        }
        return new File(dir == null ? "conf" : dir);
    }

    /** Reads the Hadoop format: &lt;configuration&gt;&lt;property&gt;&lt;name/&gt;&lt;value/&gt;&lt;/property&gt;... */
    public static Map<String, String> parseXml(File f) {
        Map<String, String> result = new HashMap<>();
        try {
            NodeList props = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(f)
                    .getDocumentElement().getElementsByTagName("property");
            for (int i = 0; i < props.getLength(); i++) {
                Element p = (Element) props.item(i);
                NodeList name = p.getElementsByTagName("name");
                NodeList value = p.getElementsByTagName("value");
                if (name.getLength() > 0 && value.getLength() > 0) {
                    result.put(name.item(0).getTextContent().trim(), value.item(0).getTextContent().trim());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot parse configuration " + f + ": " + e.getMessage(), e);
        }
        return result;
    }

    /** Value of a key: system property takes precedence over XML. null = not set. */
    public static String get(String key) {
        String v = System.getProperty(key);
        if (v == null) {
            v = CONF.get(key);
        }
        return v == null ? null : v.trim();
    }

    private static String get(String key, String legacyKey) {
        String v = System.getProperty(legacyKey);
        return v != null ? v.trim() : get(key);
    }

    private static long getLong(String key, String legacyKey, long def) {
        String v = get(key, legacyKey);
        return v == null ? def : Long.parseLong(v);
    }

    /** Like Configuration.getLongBytes: allows suffixes k, m, g, t (e.g. "128m"). */
    private static long getLongBytes(String key, String legacyKey, long def) {
        String v = get(key, legacyKey);
        return v == null ? def : parseBytes(v);
    }

    /**
     * Like Configuration.getTimeDuration: value with suffix ms, s, m, h, d (e.g. "3s");
     * without a suffix the key's unit applies ({@code unitMs} milliseconds per unit).
     * A short key that is set (legacyMsKey) is always in milliseconds. key == null: there is
     * no Hadoop key (a constant in the original), only the short key.
     */
    private static long getTimeMs(String key, long unitMs, String legacyMsKey, long defMs) {
        String legacy = System.getProperty(legacyMsKey);
        if (legacy != null) {
            return Long.parseLong(legacy.trim());
        }
        String v = key == null ? null : get(key);
        return v == null ? defMs : parseDurationMs(v, unitMs);
    }

    static long parseBytes(String v) {
        String s = v.trim().toLowerCase();
        int shift = "kmgt".indexOf(s.charAt(s.length() - 1));
        if (shift < 0) {
            return Long.parseLong(s);
        }
        return Long.parseLong(s.substring(0, s.length() - 1).trim()) << (10 * (shift + 1));
    }

    static long parseDurationMs(String v, long unitMs) {
        String s = v.trim().toLowerCase();
        String[][] suffixes = {{"ms", "1"}, {"s", "1000"}, {"m", "60000"}, {"h", "3600000"}, {"d", "86400000"}};
        for (String[] suf : suffixes) {
            if (s.endsWith(suf[0])) {
                return Long.parseLong(s.substring(0, s.length() - suf[0].length()).trim()) * Long.parseLong(suf[1]);
            }
        }
        return Long.parseLong(s) * unitMs;
    }

    /** Comma-separated list (like Configuration.getTrimmedStrings). */
    private static List<String> getTrimmedStrings(String key, String def) {
        String v = get(key);
        List<String> r = new ArrayList<>();
        for (String s : (v == null ? def : v).split(",")) {
            if (!s.isBlank()) {
                r.add(s.trim());
            }
        }
        return Collections.unmodifiableList(r);
    }

    // ----------------------------------------------------------------- Data
    /** dfs.bytes-per-checksum (original: 512). */
    public static final int BYTES_PER_CHECKSUM = 512;
    /** dfs.client-write-packet-size (original: 64 KB). */
    public static final int PACKET_SIZE = 64 * 1024;
    /** dfs.blocksize (original: 128 MB). */
    public static final long DEFAULT_BLOCK_SIZE = getLongBytes("dfs.blocksize", "edudfs.blocksize", 1024 * 1024);
    /** dfs.replication (original: 3). */
    public static final short DEFAULT_REPLICATION = (short) getLong("dfs.replication", "edudfs.replication", 3);
    /** dfs.namenode.replication.min (original: 1). */
    public static final int MIN_REPLICATION = 1;
    /** dfs.client.write.max-packets-in-flight (original: 80). */
    public static final int MAX_PACKETS_IN_FLIGHT = 80;

    // ------------------------------------------------------------ DataNode
    /**
     * dfs.datanode.data.dir: the volumes (disks) of each DataNode, optionally with a [StorageType] prefix.
     * The paths are the same for all DataNodes. Each DataNode resolves them relative to its own
     * root directory (simulated machine, see DataNode). Original default:
     * file://${hadoop.tmp.dir}/dfs/data.
     */
    public static final List<String> DATANODE_DATA_DIRS =
            getTrimmedStrings("dfs.datanode.data.dir", "[DISK]/data/1/dfs/dn");
    /** dfs.datanode.failed.volumes.tolerated (original: 0 - a single broken disk already stops the DataNode). */
    public static final int FAILED_VOLUMES_TOLERATED =
            (int) getLong("dfs.datanode.failed.volumes.tolerated", "edudfs.failed.volumes.tolerated", 0);
    /** dfs.heartbeat.interval (unit: seconds, original: 3 s). */
    public static final long HEARTBEAT_INTERVAL_MS =
            getTimeMs("dfs.heartbeat.interval", 1000, "edudfs.heartbeat.interval.ms", 1000);
    /**
     * After this time without a heartbeat a DataNode is considered dead.
     * Original: 2 * dfs.namenode.heartbeat.recheck-interval + 10 * heartbeat = 10.5 min.
     */
    public static final long HEARTBEAT_EXPIRE_MS = getTimeMs(null, 1, "edudfs.heartbeat.expire.ms", 6000);
    /** dfs.blockreport.intervalMsec (original: 6 h). */
    public static final long BLOCK_REPORT_INTERVAL_MS =
            getTimeMs("dfs.blockreport.intervalMsec", 1, "edudfs.blockreport.interval.ms", 30000);

    // -------------------------------------------------------------- Leases
    /** HdfsConstants.LEASE_SOFTLIMIT_PERIOD (original: 60 s). */
    public static final long LEASE_SOFTLIMIT_MS = getTimeMs(null, 1, "edudfs.lease.softlimit.ms", 5000);
    /** HdfsConstants.LEASE_HARDLIMIT_PERIOD (original: 20 min). */
    public static final long LEASE_HARDLIMIT_MS = getTimeMs(null, 1, "edudfs.lease.hardlimit.ms", 15000);
    /** dfs.namenode.lease-recheck-interval-ms (original: 2 s). */
    public static final long LEASE_RECHECK_INTERVAL_MS = 1000;
    /** How long a triggered block recovery is considered "in progress". */
    public static final long BLOCK_RECOVERY_TIMEOUT_MS =
            getTimeMs(null, 1, "edudfs.blockrecovery.timeout.ms", 10000);

    // ---------------------------------------------------- Re-replication
    /** dfs.namenode.redundancy.interval.seconds (original: 3 s). */
    public static final long REDUNDANCY_INTERVAL_MS = 2000;
    /** dfs.namenode.reconstruction.pending.timeout-sec (original: 5 min). */
    public static final long PENDING_RECONSTRUCTION_TIMEOUT_MS = 15000;
    /** dfs.namenode.replication.max-streams (original: 2). */
    public static final int MAX_REPLICATION_STREAMS = 2;
    /** dfs.block.invalidate.limit (original: 1000). */
    public static final int BLOCK_INVALIDATE_LIMIT = 1000;

    // -------------------------------------------------------------- Network
    /**
     * dfs.client.socket-timeout: read timeout on the data transfer connections (original: 60 s).
     * An idle write pipeline sends heartbeat packets every half of it.
     */
    public static final long SOCKET_TIMEOUT_MS = getTimeMs("dfs.client.socket-timeout", 1, "edudfs.socket.timeout.ms", 60000);
    /** fs.defaultFS from core-site.xml (original port: 8020 or 9000). */
    private static final URI DEFAULT_FS = URI.create(get("fs.defaultFS") == null ? "hdfs://localhost:9000"
            : get("fs.defaultFS"));
    public static final String NAMENODE_HOST = System.getProperty("edudfs.namenode.host", DEFAULT_FS.getHost());
    public static final int NAMENODE_RPC_PORT = Integer.getInteger("edudfs.namenode.port",
            DEFAULT_FS.getPort() > 0 ? DEFAULT_FS.getPort() : 9000);
}
