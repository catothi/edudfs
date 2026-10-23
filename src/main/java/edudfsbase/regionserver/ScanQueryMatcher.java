package edudfsbase.regionserver;

import edudfsbase.Cell;
import edudfsbase.HConstants;
import edudfsbase.KeyValue;
import edudfsbase.io.TimeRange;
import edudfsbase.util.Bytes;

import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher (with its
 * subclasses UserScanQueryMatcher, RawScanQueryMatcher, MinorCompactionScanQueryMatcher,
 * MajorCompactionScanQueryMatcher and the trackers ScanDeleteTracker / ColumnTracker in one class).
 *
 * Decides for each cell of the merged stream what happens to it. It relies on the sort order:
 * within a row and column the newest version comes first, and a delete marker comes before the
 * Puts it covers. One pass is therefore enough:
 * <pre>
 *   row1/cf:/   100/DeleteFamily      -&gt; remember: everything in the family with ts &lt;= 100 is deleted
 *   row1/cf:a/  120/Put               -&gt; INCLUDE (newer than the DeleteFamily)
 *   row1/cf:a/   90/Put               -&gt; covered -&gt; SKIP
 *   row1/cf:b/  110/DeleteColumn      -&gt; remember for column b: ts &lt;= 110 deleted
 *   row1/cf:b/  105/Put               -&gt; covered -&gt; SKIP
 * </pre>
 * Note (as in the original): a delete marker also hides Puts written LATER with an older or equal
 * timestamp - "deletes mask puts", until a major compaction has removed the marker.
 */
public class ScanQueryMatcher {

    public enum MatchCode {
        /** Return the cell (scan) or keep it (compaction). */
        INCLUDE,
        /** Hide/drop the cell, continue with the next one. */
        SKIP,
        /** The rest of this column is not needed - seek to the next column. */
        SEEK_NEXT_COL
    }

    /** Corresponds to org.apache.hadoop.hbase.regionserver.ScanType. */
    public enum ScanType {
        USER_SCAN,
        /** Minor compaction (not all files): delete markers must stay, older files may contain covered cells. */
        COMPACT_RETAIN_DELETES,
        /** Major compaction (all files): markers and everything they cover disappear for good. */
        COMPACT_DROP_DELETES
    }

    private final ScanType scanType;
    private final boolean raw;
    private final int maxVersions;
    private final TimeRange timeRange;
    private final long oldestUnexpiredTS;
    /** null = all columns of the family. */
    private final NavigableSet<String> columns;

    // state per row
    private byte[] currentRow;
    private long familyDeleteTs;
    // state per column
    private byte[] currentQualifier;
    private long columnDeleteTs;
    private final Set<Long> versionDeletes = new HashSet<>();
    private int versionCount;

    // EduDFSBase: why compactions drop cells
    private long droppedVersions;
    private long droppedDeletedCells;
    private long droppedDeleteMarkers;
    private long droppedExpired;

    /**
     * @param ttlSeconds TTL of the family (HConstants.FOREVER = none)
     * @param now        reference time for the TTL
     */
    public ScanQueryMatcher(ScanType scanType, boolean raw, int maxVersions, TimeRange timeRange, long ttlSeconds,
                            long now, NavigableSet<String> columns) {
        this.scanType = scanType;
        this.raw = raw;
        this.maxVersions = maxVersions;
        this.timeRange = timeRange;
        this.oldestUnexpiredTS = ttlSeconds == HConstants.FOREVER ? Long.MIN_VALUE : now - ttlSeconds * 1000;
        this.columns = columns == null || columns.isEmpty() ? null : columns;
    }

    public MatchCode match(Cell c) {
        if (currentRow == null || !Bytes.equals(currentRow, c.getRow())) {
            currentRow = c.getRow();
            familyDeleteTs = Long.MIN_VALUE;
            currentQualifier = null;
        }
        if (currentQualifier == null || !Bytes.equals(currentQualifier, c.getQualifier())) {
            currentQualifier = c.getQualifier();
            columnDeleteTs = Long.MIN_VALUE;
            versionDeletes.clear();
            versionCount = 0;
        }
        long ts = c.getTimestamp();
        KeyValue.Type type = c.getType();

        if (raw) {
            // Raw scan: no delete processing, only time range and number of versions
            if (!timeRange.withinTimeRange(ts) || !columnRequested(c)) {
                return MatchCode.SKIP;
            }
            return ++versionCount <= maxVersions ? MatchCode.INCLUDE : MatchCode.SKIP;
        }
        if (ts < oldestUnexpiredTS) {
            if (scanType != ScanType.USER_SCAN) {
                droppedExpired++;
            }
            return MatchCode.SKIP;  // TTL expired: invisible, a compaction drops it
        }
        if (type.isDelete()) {
            switch (type) {
                case DeleteFamily -> familyDeleteTs = Math.max(familyDeleteTs, ts);
                case DeleteColumn -> columnDeleteTs = Math.max(columnDeleteTs, ts);
                default -> versionDeletes.add(ts);
            }
            if (scanType == ScanType.COMPACT_RETAIN_DELETES) {
                return MatchCode.INCLUDE;  // minor compaction keeps the marker
            }
            if (scanType == ScanType.COMPACT_DROP_DELETES) {
                droppedDeleteMarkers++;
            }
            return MatchCode.SKIP;  // a user never sees markers
        }
        if (ts <= familyDeleteTs || ts <= columnDeleteTs || versionDeletes.contains(ts)) {
            if (scanType != ScanType.USER_SCAN) {
                droppedDeletedCells++;
            }
            return MatchCode.SKIP;
        }
        if (!columnRequested(c)) {
            return MatchCode.SEEK_NEXT_COL;
        }
        if (!timeRange.withinTimeRange(ts)) {
            return MatchCode.SKIP;
        }
        if (++versionCount > maxVersions) {
            if (scanType != ScanType.USER_SCAN) {
                droppedVersions++;
                return MatchCode.SKIP;
            }
            return MatchCode.SEEK_NEXT_COL;
        }
        return MatchCode.INCLUDE;
    }

    private boolean columnRequested(Cell c) {
        return columns == null || columns.contains(Bytes.toStringBinary(c.getQualifier()));
    }

    public long getDroppedVersions() {
        return droppedVersions;
    }

    public long getDroppedDeletedCells() {
        return droppedDeletedCells;
    }

    public long getDroppedDeleteMarkers() {
        return droppedDeleteMarkers;
    }

    public long getDroppedExpired() {
        return droppedExpired;
    }
}
