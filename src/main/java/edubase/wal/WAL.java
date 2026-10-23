package edubase.wal;

import edubase.RegionInfo;

import java.io.Closeable;
import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.wal.WAL: the write-ahead log of a RegionServer.
 *
 * Every edit goes into the WAL (in HDFS, i.e. replicated) BEFORE it goes into the MemStore. If the
 * RegionServer crashes, the MemStore is lost - but its content can be reconstructed from the WAL.
 * Implementation: {@link FSHLog}.
 */
public interface WAL extends Closeable {

    /** Appends an entry (still buffered). Returns the sequence ID. */
    long appendData(RegionInfo region, WALKeyImpl key, WALEdit edit) throws IOException;

    /** Makes everything appended so far durable (hflush to all DataNodes of the pipeline). */
    void sync() throws IOException;

    /** Closes the current WAL file and starts a new one. */
    void rollWriter() throws IOException;

    /**
     * A region has flushed everything up to flushedSeqId into HFiles. WAL files that contain
     * nothing newer from any region are no longer needed and go to oldWALs.
     */
    void completeCacheFlush(String encodedRegionName, long flushedSeqId);

    /** An entry: key + cells. */
    final class Entry {
        private final WALKeyImpl key;
        private final WALEdit edit;

        public Entry(WALKeyImpl key, WALEdit edit) {
            this.key = key;
            this.edit = edit;
        }

        public WALKeyImpl getKey() {
            return key;
        }

        public WALEdit getEdit() {
            return edit;
        }
    }

    /** Reads WAL entries one after another (corresponds to WAL.Reader / ProtobufLogReader). */
    interface Reader extends Closeable {
        /** The next entry; null at the end (or at an entry the crash cut off). */
        Entry next() throws IOException;
    }
}
