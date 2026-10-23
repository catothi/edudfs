package edubase.regionserver;

import edubase.Cell;
import edubase.client.Scan;
import edubase.io.hfile.HFileScanner;
import edubase.util.Bytes;
import edubase.util.ReadTrace;

import java.io.IOException;
import java.util.NavigableSet;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.StoreFileScanner: KeyValueScanner over one
 * HFile. Before it is used at all, {@link #shouldUseScanner} checks whether the file can be
 * relevant for the scan - this is where the Bloom filter takes effect.
 */
public class StoreFileScanner implements KeyValueScanner {

    private final HStoreFile file;
    private final HFileScanner hfs;
    private final ReadTrace trace;
    private Cell cur;
    private boolean closed;
    /** The ROW Bloom filter said "maybe" - the first seek shows whether that was right. */
    private boolean bloomSaidMaybe;

    public StoreFileScanner(HStoreFile file, boolean cacheBlocks, ReadTrace trace) {
        this.file = file;
        this.hfs = file.getReader().getHFileReader().getScanner(cacheBlocks, trace);
        this.trace = trace;
        file.increaseRefCount();
    }

    /**
     * Corresponds to StoreFileScanner.shouldUseScanner: key range, time range and (for a Get) Bloom.
     * Every decision is recorded in the trace.
     */
    public boolean shouldUseScanner(Scan scan, NavigableSet<String> columns, long oldestUnexpiredTS) {
        StoreFileReader r = file.getReader();
        String name = "HFile " + file.getShortName();
        if (!r.passesTimerangeFilter(scan.getTimeRange(), oldestUnexpiredTS)) {
            trace(name + ": time range " + r.getMinTimestamp() + ".." + r.getMaxTimestamp()
                    + " outside of the requested range -> skipped");
            return false;
        }
        if (!r.passesKeyRangeFilter(scan)) {
            trace(name + ": key range of the file does not overlap the requested rows -> skipped");
            return false;
        }
        if (scan.isGetScan() && r.getBloomFilterType() != BloomType.NONE) {
            byte[] qualifier = null;
            if (r.getBloomFilterType() == BloomType.ROWCOL && columns != null && columns.size() == 1) {
                qualifier = Bytes.toBytesBinary(columns.first());
            }
            boolean maybe = r.passesBloomFilter(scan.getStartRow(), qualifier);
            if (!maybe) {
                trace(name + ": Bloom filter (" + r.getBloomFilterType() + ") says NO -> skipped, no block read");
                return false;
            }
            trace(name + ": Bloom filter (" + r.getBloomFilterType() + ") says MAYBE -> must read");
            bloomSaidMaybe = qualifier == null;
        } else if (scan.isGetScan()) {
            trace(name + ": no Bloom filter -> must read");
        } else {
            trace(name + ": overlaps the scan range -> read");
        }
        return true;
    }

    private void trace(String s) {
        if (trace != null) {
            trace.add("  " + s);
        }
    }

    @Override
    public Cell peek() {
        return cur;
    }

    @Override
    public Cell next() throws IOException {
        Cell c = cur;
        cur = hfs.next() ? hfs.getCell() : null;
        return c;
    }

    @Override
    public boolean seek(Cell key) throws IOException {
        trace("HFile " + file.getShortName() + ": seek");
        cur = hfs.seekTo(key) ? hfs.getCell() : null;
        checkFalsePositive(key);
        return cur != null;
    }

    @Override
    public boolean reseek(Cell key) throws IOException {
        cur = hfs.reseekTo(key) ? hfs.getCell() : null;
        return cur != null;
    }

    /** After the Bloom said "maybe" for a row, the seek shows whether the row is really there. */
    private void checkFalsePositive(Cell key) {
        if (bloomSaidMaybe && (cur == null || !Bytes.equals(cur.getRow(), key.getRow()))) {
            file.getReader().countFalsePositive();
            trace("HFile " + file.getShortName() + ": row not in the file after all -> Bloom FALSE POSITIVE");
        }
        bloomSaidMaybe = false;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            cur = null;
            file.decreaseRefCount();
        }
    }

    @Override
    public String getSourceName() {
        return "HFile " + file.getShortName();
    }
}
