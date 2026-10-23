package edudfsbase.client;

import edudfsbase.io.TimeRange;
import edudfsbase.util.Bytes;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Get: reads ONE row (optionally only certain
 * families/columns, several versions, a time range).
 *
 * A Get is the case for which Bloom filters exist: the row is known exactly, so every HFile
 * whose Bloom filter says "not contained" can be skipped.
 */
public class Get implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] row;
    private final Map<String, NavigableSet<String>> familyMap = new TreeMap<>();
    private int maxVersions = 1;
    private TimeRange timeRange = TimeRange.ALL_TIME;
    private boolean trace;
    private boolean showSource;

    public Get(byte[] row) {
        this.row = row;
    }

    public Get addFamily(byte[] family) {
        familyMap.put(Bytes.toString(family), new TreeSet<>());
        return this;
    }

    public Get addColumn(byte[] family, byte[] qualifier) {
        familyMap.computeIfAbsent(Bytes.toString(family), k -> new TreeSet<>()).add(Bytes.toStringBinary(qualifier));
        return this;
    }

    public Get readVersions(int versions) {
        this.maxVersions = versions;
        return this;
    }

    public Get setTimeRange(long minStamp, long maxStamp) {
        this.timeRange = new TimeRange(minStamp, maxStamp);
        return this;
    }

    /** EduDFSBase: record the read path (MemStore, Bloom filters, block index, BlockCache). */
    public Get setTrace(boolean trace) {
        this.trace = trace;
        return this;
    }

    public Get setShowSource(boolean showSource) {
        this.showSource = showSource;
        return this;
    }

    public byte[] getRow() {
        return row;
    }

    public Map<String, NavigableSet<String>> getFamilyMap() {
        return familyMap;
    }

    public int getMaxVersions() {
        return maxVersions;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public boolean isTrace() {
        return trace;
    }

    public boolean isShowSource() {
        return showSource;
    }
}
