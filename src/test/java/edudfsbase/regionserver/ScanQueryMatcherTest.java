package edudfsbase.regionserver;

import edudfsbase.HConstants;
import edudfsbase.KeyValue;
import edudfsbase.io.TimeRange;
import edudfsbase.regionserver.ScanQueryMatcher.MatchCode;
import edudfsbase.regionserver.ScanQueryMatcher.ScanType;
import edudfsbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Versions, delete markers, TTL and the three scan types - cell by cell, in sort order. */
class ScanQueryMatcherTest {

    private static final byte[] CF = Bytes.toBytes("cf");

    private static KeyValue kv(String row, String q, long ts, KeyValue.Type t) {
        return new KeyValue(Bytes.toBytes(row), CF, q == null ? null : Bytes.toBytes(q), ts, t, Bytes.toBytes("v"));
    }

    /** A row as it arrives sorted from the KeyValueHeap. */
    private static List<KeyValue> row() {
        return List.of(
                kv("r", null, 100, KeyValue.Type.DeleteFamily),  // everything <= 100 deleted
                kv("r", "a", 300, KeyValue.Type.Put),
                kv("r", "a", 200, KeyValue.Type.Put),
                kv("r", "a", 150, KeyValue.Type.Put),
                kv("r", "a", 50, KeyValue.Type.Put),             // covered by DeleteFamily
                kv("r", "b", 250, KeyValue.Type.DeleteColumn),   // column b: everything <= 250 deleted
                kv("r", "b", 240, KeyValue.Type.Put),
                kv("r", "c", 180, KeyValue.Type.Delete),         // exactly this version
                kv("r", "c", 180, KeyValue.Type.Put),
                kv("r", "c", 170, KeyValue.Type.Put));
    }

    private static List<MatchCode> run(ScanQueryMatcher m) {
        List<MatchCode> r = new ArrayList<>();
        for (KeyValue c : row()) {
            r.add(m.match(c));
        }
        return r;
    }

    private static final MatchCode I = MatchCode.INCLUDE;
    private static final MatchCode S = MatchCode.SKIP;
    private static final MatchCode NEXT_COL = MatchCode.SEEK_NEXT_COL;

    @Test
    void userScanHidesMarkersAndCoveredCellsAndLimitsVersions() {
        ScanQueryMatcher m = new ScanQueryMatcher(ScanType.USER_SCAN, false, 2, TimeRange.ALL_TIME,
                HConstants.FOREVER, 1000, null);
        assertEquals(List.of(S, I, I, NEXT_COL, S, S, S, S, S, I), run(m));
    }

    @Test
    void rawScanShowsEverything() {
        ScanQueryMatcher m = new ScanQueryMatcher(ScanType.USER_SCAN, true, 10, TimeRange.ALL_TIME,
                HConstants.FOREVER, 1000, null);
        assertEquals(List.of(I, I, I, I, I, I, I, I, I, I), run(m));
    }

    @Test
    void minorCompactionKeepsMarkersButDropsCoveredCellsAndOldVersions() {
        ScanQueryMatcher m = new ScanQueryMatcher(ScanType.COMPACT_RETAIN_DELETES, false, 2, TimeRange.ALL_TIME,
                HConstants.FOREVER, 1000, null);
        assertEquals(List.of(I, I, I, S, S, I, S, I, S, I), run(m));
        assertEquals(1, m.getDroppedVersions());
        assertEquals(3, m.getDroppedDeletedCells());
        assertEquals(0, m.getDroppedDeleteMarkers());
    }

    @Test
    void majorCompactionDropsMarkersToo() {
        ScanQueryMatcher m = new ScanQueryMatcher(ScanType.COMPACT_DROP_DELETES, false, 2, TimeRange.ALL_TIME,
                HConstants.FOREVER, 1000, null);
        assertEquals(List.of(S, I, I, S, S, S, S, S, S, I), run(m));
        assertEquals(3, m.getDroppedDeleteMarkers());
    }

    @Test
    void ttlExpiresOldCells() {
        // now = 1200 ms, TTL = 1 s -> everything older than 200 ms is expired
        ScanQueryMatcher m = new ScanQueryMatcher(ScanType.COMPACT_DROP_DELETES, false, 10, TimeRange.ALL_TIME,
                1, 1200, null);
        assertEquals(I, m.match(kv("r", "a", 300, KeyValue.Type.Put)));
        assertEquals(S, m.match(kv("r", "a", 150, KeyValue.Type.Put)));
        assertEquals(1, m.getDroppedExpired());
    }

    @Test
    void onlyRequestedColumns() {
        TreeSet<String> cols = new TreeSet<>(List.of("c"));
        ScanQueryMatcher m = new ScanQueryMatcher(ScanType.USER_SCAN, false, 1, TimeRange.ALL_TIME,
                HConstants.FOREVER, 1000, cols);
        assertEquals(List.of(S, NEXT_COL, NEXT_COL, NEXT_COL, S, S, S, S, S, I), run(m));
    }
}
