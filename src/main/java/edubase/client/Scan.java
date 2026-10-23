package edubase.client;

import edubase.HBaseConfiguration;
import edubase.HConstants;
import edubase.io.TimeRange;
import edubase.util.Bytes;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Scan: reads rows in key order from startRow
 * (inclusive) to stopRow (exclusive). A {@link Get} is internally a scan over exactly one row.
 *
 * EduBase extensions: {@link #setTrace} (record the read path) and {@link #setShowSource}
 * (for every cell: from the MemStore or from which HFile).
 */
public class Scan implements Serializable {

    private static final long serialVersionUID = 1L;

    private byte[] startRow = HConstants.EMPTY_START_ROW;
    private byte[] stopRow = HConstants.EMPTY_END_ROW;
    private boolean includeStopRow;
    /** family -&gt; qualifiers (empty set = all columns of the family); empty map = all families. */
    private final Map<String, NavigableSet<String>> familyMap = new TreeMap<>();
    private int maxVersions = 1;
    private TimeRange timeRange = TimeRange.ALL_TIME;
    private boolean raw;
    private int limit = -1;
    private int caching = HBaseConfiguration.SCANNER_CACHING;
    private boolean getScan;
    private boolean trace;
    private boolean showSource;

    public Scan() {
    }

    /** Scan for a Get: exactly one row, startRow = stopRow (inclusive). */
    public Scan(Get get) {
        this.startRow = get.getRow();
        this.stopRow = get.getRow();
        this.includeStopRow = true;
        this.getScan = true;
        this.maxVersions = get.getMaxVersions();
        this.timeRange = get.getTimeRange();
        this.familyMap.putAll(get.getFamilyMap());
        this.trace = get.isTrace();
        this.showSource = get.isShowSource();
    }

    public Scan withStartRow(byte[] startRow) {
        this.startRow = startRow;
        return this;
    }

    public Scan withStopRow(byte[] stopRow) {
        this.stopRow = stopRow;
        return this;
    }

    public Scan addFamily(byte[] family) {
        familyMap.put(Bytes.toString(family), new TreeSet<>());
        return this;
    }

    public Scan addColumn(byte[] family, byte[] qualifier) {
        familyMap.computeIfAbsent(Bytes.toString(family), k -> new TreeSet<>()).add(Bytes.toStringBinary(qualifier));
        return this;
    }

    public Scan readVersions(int versions) {
        this.maxVersions = versions;
        return this;
    }

    public Scan readAllVersions() {
        return readVersions(Integer.MAX_VALUE);
    }

    public Scan setTimeRange(long minStamp, long maxStamp) {
        this.timeRange = new TimeRange(minStamp, maxStamp);
        return this;
    }

    /** Raw scan: also returns delete markers and the cells they cover (no delete processing). */
    public Scan setRaw(boolean raw) {
        this.raw = raw;
        return this;
    }

    public Scan setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    /** Rows per next() RPC between client and RegionServer. */
    public Scan setCaching(int caching) {
        this.caching = caching;
        return this;
    }

    public Scan setTrace(boolean trace) {
        this.trace = trace;
        return this;
    }

    public Scan setShowSource(boolean showSource) {
        this.showSource = showSource;
        return this;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public byte[] getStopRow() {
        return stopRow;
    }

    public boolean includeStopRow() {
        return includeStopRow;
    }

    public Map<String, NavigableSet<String>> getFamilyMap() {
        return familyMap;
    }

    public boolean hasFamilies() {
        return !familyMap.isEmpty();
    }

    public int getMaxVersions() {
        return maxVersions;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public boolean isRaw() {
        return raw;
    }

    public int getLimit() {
        return limit;
    }

    public int getCaching() {
        return caching;
    }

    public boolean isGetScan() {
        return getScan;
    }

    public boolean isTrace() {
        return trace;
    }

    public boolean isShowSource() {
        return showSource;
    }

    /** Copy for the next region: the client continues the scan at startRow. */
    Scan copyWithStartRow(byte[] newStart) {
        Scan s = new Scan();
        s.startRow = newStart;
        s.stopRow = stopRow;
        s.includeStopRow = includeStopRow;
        s.familyMap.putAll(familyMap);
        s.maxVersions = maxVersions;
        s.timeRange = timeRange;
        s.raw = raw;
        s.limit = limit;
        s.caching = caching;
        s.trace = trace;
        s.showSource = showSource;
        return s;
    }
}
