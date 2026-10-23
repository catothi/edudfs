package edudfsbase.regionserver;

import edudfsbase.Cell;
import edudfsbase.HBaseConfiguration;
import edudfsbase.KeyValue;
import edudfsbase.client.ColumnFamilyDescriptor;
import edudfsbase.io.hfile.HFile;
import edudfsbase.io.hfile.HFileInfo;
import edudfsbase.util.BloomFilterChunk;
import edudfsbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.StoreFileWriter: writes an HFile for a store
 * and adds what the store needs beyond the bare format:
 * <ul>
 *   <li>the Bloom filter (type from the family: ROW key = row, ROWCOL key = row + qualifier),</li>
 *   <li>the time range of all cells (TIMERANGE),</li>
 *   <li>MAX_SEQ_ID_KEY: up to which sequence ID the edits are contained - needed to decide which
 *       WAL edits must be replayed after a crash,</li>
 *   <li>MAJOR_COMPACTION_KEY and the names of the compacted input files.</li>
 * </ul>
 */
public class StoreFileWriter implements Closeable {

    private final HFile.Writer writer;
    private final BloomType bloomType;
    private final BloomFilterChunk bloom;
    private byte[] lastBloomKey;
    private long minTs = Long.MAX_VALUE;
    private long maxTs = Long.MIN_VALUE;
    private long earliestPutTs = Long.MAX_VALUE;
    private long deleteFamilyCnt;
    private long maxMemstoreTs;

    /**
     * @param maxKeys expected number of cells; determines the size of the Bloom filter
     *                (the original also passes this estimate when creating the writer)
     */
    public StoreFileWriter(OutputStream out, String name, ColumnFamilyDescriptor family, long maxKeys) {
        this.writer = HFile.getWriter(out, name, family.getBlocksize());
        this.bloomType = family.getBloomFilterType();
        this.bloom = bloomType == BloomType.NONE ? null
                : new BloomFilterChunk(Math.max(1, maxKeys), HBaseConfiguration.BLOOM_ERROR_RATE);
    }

    public void append(Cell cell) throws IOException {
        writer.append(cell);
        if (bloom != null) {
            byte[] key = bloomType == BloomType.ROW ? cell.getRow() : Bytes.add(cell.getRow(), cell.getQualifier());
            if (lastBloomKey == null || !Bytes.equals(key, lastBloomKey)) {
                bloom.add(key);  // consecutive duplicates (several versions/columns) only once
                lastBloomKey = key;
            }
        }
        minTs = Math.min(minTs, cell.getTimestamp());
        maxTs = Math.max(maxTs, cell.getTimestamp());
        if (cell.getType() == KeyValue.Type.Put) {
            earliestPutTs = Math.min(earliestPutTs, cell.getTimestamp());
        } else if (cell.getType() == KeyValue.Type.DeleteFamily) {
            deleteFamilyCnt++;
        }
        maxMemstoreTs = Math.max(maxMemstoreTs, cell.getSequenceId());
    }

    /** Corresponds to appendMetadata(maxSequenceId, majorCompaction) + appendCompactionEvent. */
    public void appendMetadata(long maxSequenceId, boolean majorCompaction, String createdBy,
                               List<String> compactedFiles) {
        writer.appendFileInfo(HFileInfo.MAX_SEQ_ID_KEY, String.valueOf(maxSequenceId));
        writer.appendFileInfo(HFileInfo.MAJOR_COMPACTION_KEY, String.valueOf(majorCompaction));
        writer.appendFileInfo(HFileInfo.EDU_CREATED_BY, createdBy);
        if (compactedFiles != null && !compactedFiles.isEmpty()) {
            writer.appendFileInfo(HFileInfo.COMPACTION_EVENT_KEY, String.join(",", compactedFiles));
        }
    }

    public long getEntryCount() {
        return writer.getEntryCount();
    }

    @Override
    public void close() throws IOException {
        writer.appendFileInfo(HFileInfo.BLOOM_FILTER_TYPE_KEY, bloomType.name());
        writer.appendFileInfo(HFileInfo.TIMERANGE_KEY, writer.getEntryCount() == 0 ? "0..0" : minTs + ".." + maxTs);
        writer.appendFileInfo(HFileInfo.EARLIEST_PUT_TS,
                String.valueOf(earliestPutTs == Long.MAX_VALUE ? -1 : earliestPutTs));
        writer.appendFileInfo(HFileInfo.DELETE_FAMILY_COUNT, String.valueOf(deleteFamilyCnt));
        writer.appendFileInfo(HFileInfo.MAX_MEMSTORE_TS_KEY, String.valueOf(maxMemstoreTs));
        writer.appendFileInfo(HFileInfo.KEY_VALUE_VERSION, "1");
        writer.setGeneralBloomFilter(bloom);
        writer.close();
    }
}
