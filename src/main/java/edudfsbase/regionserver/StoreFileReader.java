package edudfsbase.regionserver;

import edudfsbase.CellComparator;
import edudfsbase.KeyValue;
import edudfsbase.client.Scan;
import edudfsbase.io.TimeRange;
import edudfsbase.io.hfile.HFile;
import edudfsbase.io.hfile.HFileInfo;
import edudfsbase.util.BloomFilterChunk;
import edudfsbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.StoreFileReader: the HFile reader plus the
 * filters with which a scan decides whether it needs to look into a file at all:
 * key range (first/last key), time range and - for a Get - the Bloom filter.
 */
public class StoreFileReader {

    private final HFile.Reader reader;
    private final BloomType bloomFilterType;
    private final BloomFilterChunk generalBloomFilter;
    private final long minTs;
    private final long maxTs;

    // EduDFSBase statistics (the original has comparable metrics: bloom requests / negatives)
    private final AtomicLong bloomRequests = new AtomicLong();
    private final AtomicLong bloomNegatives = new AtomicLong();
    private final AtomicLong bloomFalsePositives = new AtomicLong();

    public StoreFileReader(HFile.Reader reader) {
        this.reader = reader;
        HFileInfo fi = reader.getHFileInfo();
        String bt = fi.get(HFileInfo.BLOOM_FILTER_TYPE_KEY);
        this.bloomFilterType = bt == null ? BloomType.NONE : BloomType.valueOf(bt);
        this.generalBloomFilter = reader.getGeneralBloomFilter();
        String tr = fi.get(HFileInfo.TIMERANGE_KEY);
        if (tr == null) {
            minTs = 0;
            maxTs = Long.MAX_VALUE;
        } else {
            String[] p = tr.split("\\.\\.");
            minTs = Long.parseLong(p[0]);
            maxTs = Long.parseLong(p[1]);
        }
    }

    public HFile.Reader getHFileReader() {
        return reader;
    }

    public BloomType getBloomFilterType() {
        return bloomFilterType;
    }

    /** Bloom check for row (ROW) or row+qualifier (ROWCOL). true = "maybe", false = "definitely not". */
    public boolean passesBloomFilter(byte[] row, byte[] qualifier) {
        if (generalBloomFilter == null) {
            return true;
        }
        byte[] key;
        if (bloomFilterType == BloomType.ROW) {
            key = row;
        } else if (qualifier != null) {
            key = Bytes.add(row, qualifier);
        } else {
            return true;  // ROWCOL, but the Get asks for several/all columns: the filter does not help
        }
        bloomRequests.incrementAndGet();
        boolean maybe = generalBloomFilter.contains(key);
        if (!maybe) {
            bloomNegatives.incrementAndGet();
        }
        return maybe;
    }

    /** The Bloom filter said "maybe", but the row is not in the file (counted by StoreFileScanner). */
    void countFalsePositive() {
        bloomFalsePositives.incrementAndGet();
    }

    public boolean passesTimerangeFilter(TimeRange tr, long oldestUnexpiredTS) {
        return tr.includesTimeRange(minTs, maxTs) && maxTs >= oldestUnexpiredTS;
    }

    /** Does the scan's row range overlap [first row, last row] of the file? */
    public boolean passesKeyRangeFilter(Scan scan) {
        KeyValue first = reader.getFirstKey();
        KeyValue last = reader.getLastKey();
        if (first == null) {
            return false;  // empty file
        }
        byte[] stop = scan.getStopRow();
        if (stop.length > 0) {
            int c = CellComparator.compareRows(first.getRow(), stop);
            if (c > 0 || (c == 0 && !scan.includeStopRow())) {
                return false;
            }
        }
        return CellComparator.compareRows(last.getRow(), scan.getStartRow()) >= 0;
    }

    public long getMinTimestamp() {
        return minTs;
    }

    public long getMaxTimestamp() {
        return maxTs;
    }

    public long getEntries() {
        return reader.getEntries();
    }

    public long length() {
        return reader.length();
    }

    public long getBloomRequests() {
        return bloomRequests.get();
    }

    public long getBloomNegatives() {
        return bloomNegatives.get();
    }

    public long getBloomFalsePositives() {
        return bloomFalsePositives.get();
    }

    public BloomFilterChunk getGeneralBloomFilter() {
        return generalBloomFilter;
    }

    public void close() throws IOException {
        reader.close();
    }
}
