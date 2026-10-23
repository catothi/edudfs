package edudfsbase.util;

/**
 * Corresponds to org.apache.hadoop.hbase.util.EnvironmentEdgeManager: the single source of the
 * current time in HBase (tests can replace it). Cell timestamps are milliseconds since 1970.
 */
public final class EnvironmentEdgeManager {

    private EnvironmentEdgeManager() {
    }

    public static long currentTime() {
        return System.currentTimeMillis();
    }
}
