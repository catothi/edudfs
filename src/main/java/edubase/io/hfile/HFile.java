package edubase.io.hfile;

import edubase.Cell;
import edubase.KeyValue;
import edubase.io.PositionedReadable;
import edubase.util.BloomFilterChunk;
import edubase.util.ReadTrace;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFile: the file format in which HBase stores its
 * data in HDFS. An HFile is written ONCE, sorted, and never changed afterwards (immutable);
 * new data creates new HFiles (flush), several HFiles are merged into one (compaction).
 *
 * <pre>
 *  +-----------------------------+  "scanned block section" - read by a scan from front to back
 *  | DATABLK*  data block 0      |
 *  | DATABLK*  data block 1      |
 *  | ...                         |
 *  | BLMFBLK2  Bloom chunk       |
 *  +-----------------------------+  "load-on-open section" - read completely when the file is opened
 *  | IDXROOT2  root block index  |
 *  | FILEINF2  FileInfo          |
 *  | BLMFMET2  Bloom metadata    |
 *  +-----------------------------+
 *  | TRABLK"$  trailer (fixed)   |  &lt;- the reader starts HERE (end of file - 512)
 *  +-----------------------------+
 * </pre>
 *
 * <b>EduBase writes HFiles as TEXT</b> so you can read them with {@code ./hdfs dfs -cat} or an
 * editor. Layout, block types and the order of reading are those of the original HFile version 3.
 * <b>The original is binary</b> (fixed-width numbers, KeyValue bytes, protobuf for trailer and
 * FileInfo, optionally compressed and encoded blocks); there you need {@code hbase hfile}
 * (HFilePrettyPrinter) to look inside.
 */
public final class HFile {

    private HFile() {
    }

    /** Writes an HFile; cells must arrive in CellComparator order. */
    public interface Writer extends Closeable {
        void append(Cell cell) throws IOException;

        void appendFileInfo(String key, String value);

        /** The Bloom filter written at close(); the caller (StoreFileWriter) decides which keys go in. */
        void setGeneralBloomFilter(BloomFilterChunk bloom);

        long getEntryCount();
    }

    /** Reads an HFile; the load-on-open section is in memory after opening. */
    public interface Reader extends Closeable {
        String getName();

        long length();

        FixedFileTrailer getTrailer();

        HFileInfo getHFileInfo();

        HFileBlockIndex.BlockIndexReader getDataBlockIndexReader();

        /** null if the file has no Bloom filter. */
        BloomFilterChunk getGeneralBloomFilter();

        KeyValue getFirstKey();

        KeyValue getLastKey();

        long getEntries();

        HFileBlock readBlock(long offset, boolean cacheBlock, ReadTrace trace) throws IOException;

        HFileScanner getScanner(boolean cacheBlocks, ReadTrace trace);
    }

    public static Writer getWriter(OutputStream out, String name, int blockSize) {
        return new HFileWriterImpl(out, name, blockSize);
    }

    public static Reader createReader(String name, PositionedReadable in, LruBlockCache cache) throws IOException {
        return new HFileReaderImpl(name, in, cache);
    }
}
