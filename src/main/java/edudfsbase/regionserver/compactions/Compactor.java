package edudfsbase.regionserver.compactions;

import edudfsbase.Cell;
import edudfsbase.KeyValue;
import edudfsbase.io.TimeRange;
import edudfsbase.regionserver.HStore;
import edudfsbase.regionserver.HStoreFile;
import edudfsbase.regionserver.ScanQueryMatcher;
import edudfsbase.regionserver.StoreFileScanner;
import edudfsbase.regionserver.StoreFileWriter;
import edudfsbase.regionserver.StoreScanner;
import edudfsbase.util.Bytes;
import edudfsbase.util.EnvironmentEdgeManager;
import edudfs.client.DFSOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor.
 *
 * A compaction is a merge sort: StoreFileScanners over all input files -&gt; KeyValueHeap -&gt;
 * ScanQueryMatcher (drops old versions, deleted cells, expired cells and - if all files take
 * part - the delete markers) -&gt; ONE new HFile in .tmp. The store then swaps the files.
 * The blocks read are not put into the BlockCache (they would push useful blocks out).
 */
public final class Compactor {

    private Compactor() {
    }

    /** Writes the result into a temporary file; returns its path, the details go into record. */
    public static String compact(HStore store, CompactionRequest request, CompactionRecord record) throws IOException {
        List<StoreFileScanner> scanners = new ArrayList<>();
        long maxKeys = 0;
        long maxSeqId = 0;
        List<String> inputNames = new ArrayList<>();
        for (HStoreFile f : request.getFiles()) {
            StoreFileScanner s = new StoreFileScanner(f, false, null);
            s.seek(KeyValue.createFirstOnRow(Bytes.EMPTY_BYTE_ARRAY));
            scanners.add(s);
            maxKeys += f.getReader().getEntries();
            maxSeqId = Math.max(maxSeqId, f.getMaxSequenceId());
            inputNames.add(f.getName());
            record.inputs.add(new CompactionRecord.FileSummary(f.getName(), f.length(), f.getReader().getEntries(),
                    f.getCreatedBy()));
        }
        ScanQueryMatcher.ScanType type = request.isAllFiles() ? ScanQueryMatcher.ScanType.COMPACT_DROP_DELETES
                : ScanQueryMatcher.ScanType.COMPACT_RETAIN_DELETES;
        ScanQueryMatcher matcher = new ScanQueryMatcher(type, false, store.getColumnFamilyDescriptor().getMaxVersions(),
                TimeRange.ALL_TIME, store.getColumnFamilyDescriptor().getTimeToLive(),
                EnvironmentEdgeManager.currentTime(), null);
        StoreScanner scanner = new StoreScanner(scanners, matcher);
        String tmpPath = store.getRegionFileSystem().createTempPath(store.getColumnFamilyName());
        String createdBy = (request.isMajor() ? "major" : "minor") + " compaction";
        try (DFSOutputStream out = store.getRegionFileSystem().getFileSystem().create(tmpPath, false);
             StoreFileWriter writer = new StoreFileWriter(out, tmpPath, store.getColumnFamilyDescriptor(), maxKeys)) {
            List<Cell> row = new ArrayList<>();
            boolean more;
            do {
                row.clear();
                more = scanner.next(row, null);
                for (Cell c : row) {
                    writer.append(c);
                }
            } while (more);
            writer.appendMetadata(maxSeqId, request.isAllFiles(), createdBy, inputNames);
        } finally {
            scanner.close();
        }
        record.major = request.isMajor();
        record.allFiles = request.isAllFiles();
        record.droppedVersions = matcher.getDroppedVersions();
        record.droppedDeletedCells = matcher.getDroppedDeletedCells();
        record.droppedDeleteMarkers = matcher.getDroppedDeleteMarkers();
        record.droppedExpired = matcher.getDroppedExpired();
        return tmpPath;
    }

    /** Called by the store after the commit with the opened output file. */
    public static void finish(CompactionRecord record, HStoreFile output, int remainingFiles, long startTime) {
        record.output = new CompactionRecord.FileSummary(output.getName(), output.length(),
                output.getReader().getEntries(), output.getCreatedBy());
        record.remainingFiles = remainingFiles;
        record.startTime = startTime;
        record.durationMs = EnvironmentEdgeManager.currentTime() - startTime;
    }
}
