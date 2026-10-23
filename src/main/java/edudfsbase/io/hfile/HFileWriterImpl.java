package edudfsbase.io.hfile;

import edudfsbase.Cell;
import edudfsbase.CellComparator;
import edudfsbase.KeyValue;
import edudfsbase.codec.KeyValueCodec;
import edudfsbase.util.BloomFilterChunk;
import edudfsbase.util.Bytes;
import edudfsbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFileWriterImpl.
 *
 * append(): the cells are collected in the current data block. Once it has reached the block size
 * (column family attribute BLOCKSIZE), the block is written and its first key goes into the index.
 * close(): last data block, Bloom chunk, then the load-on-open section (index, FileInfo, Bloom
 * meta) and finally the trailer, which points to the load-on-open section.
 */
public class HFileWriterImpl implements HFile.Writer {

    private final OutputStream out;
    private final String name;
    private final int blockSize;
    private long pos;

    private final StringBuilder blockBody = new StringBuilder();
    private KeyValue blockFirstKey;
    private Cell lastCell;

    private long entryCount;
    private long totalKeyLength;
    private long totalValueLength;
    private long totalUncompressedBytes;
    private long firstDataBlockOffset = -1;
    private long lastDataBlockOffset = -1;

    private final HFileBlockIndex.BlockIndexWriter dataBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
    private final HFileInfo fileInfo = new HFileInfo();
    private BloomFilterChunk bloom;
    private boolean closed;

    HFileWriterImpl(OutputStream out, String name, int blockSize) {
        this.out = out;
        this.name = name;
        this.blockSize = blockSize;
    }

    @Override
    public void append(Cell cell) throws IOException {
        if (lastCell != null && CellComparator.getInstance().compareKeys(lastCell, cell) > 0) {
            throw new IOException("Added a key not lexically larger than previous. Current cell = " + cell
                    + ", lastCell = " + lastCell);
        }
        if (blockBody.length() == 0) {
            blockFirstKey = new KeyValue(cell.getRow(), cell.getFamily(), cell.getQualifier(), cell.getTimestamp(),
                    cell.getType(), null);
        }
        blockBody.append(KeyValueCodec.encode(cell)).append('\n');
        entryCount++;
        totalKeyLength += KeyValue.KEY_INFRASTRUCTURE_SIZE + cell.getRow().length + cell.getFamily().length
                + cell.getQualifier().length;
        totalValueLength += cell.getValue().length;
        lastCell = cell;
        if (blockBody.length() >= blockSize) {
            finishBlock();
        }
    }

    private void finishBlock() throws IOException {
        if (blockBody.length() == 0) {
            return;
        }
        byte[] b = HFileBlock.serialize(BlockType.DATA, lastDataBlockOffset, blockBody.toString());
        dataBlockIndexWriter.addEntry(blockFirstKey, pos, b.length);
        if (firstDataBlockOffset < 0) {
            firstDataBlockOffset = pos;
        }
        lastDataBlockOffset = pos;
        totalUncompressedBytes += b.length;
        write(b);
        blockBody.setLength(0);
    }

    private void write(byte[] b) throws IOException {
        out.write(b);
        pos += b.length;
    }

    @Override
    public void appendFileInfo(String key, String value) {
        fileInfo.put(key, value);
    }

    @Override
    public void setGeneralBloomFilter(BloomFilterChunk bloom) {
        this.bloom = bloom;
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        finishBlock();
        long bloomChunkOffset = pos;
        int bloomChunkSize = 0;
        if (bloom != null) {
            byte[] b = HFileBlock.serialize(BlockType.BLOOM_CHUNK, -1, hexLines(bloom.getBytes()));
            bloomChunkSize = b.length;
            totalUncompressedBytes += b.length;
            write(b);
        }

        // ---- load-on-open section
        FixedFileTrailer trailer = new FixedFileTrailer();
        trailer.loadOnOpenDataOffset = pos;
        write(HFileBlock.serialize(BlockType.ROOT_INDEX, -1, dataBlockIndexWriter.toBody()));

        trailer.fileInfoOffset = pos;
        if (lastCell != null) {
            fileInfo.put(HFileInfo.LASTKEY, KeyValueCodec.encodeKey(lastCell));
        }
        fileInfo.put(HFileInfo.AVG_KEY_LEN, String.valueOf(entryCount == 0 ? 0 : totalKeyLength / entryCount));
        fileInfo.put(HFileInfo.AVG_VALUE_LEN, String.valueOf(entryCount == 0 ? 0 : totalValueLength / entryCount));
        fileInfo.put(HFileInfo.CREATE_TIME_KEY, String.valueOf(EnvironmentEdgeManager.currentTime()));
        write(HFileBlock.serialize(BlockType.FILE_INFO, -1, fileInfo.toBody()));

        if (bloom != null) {
            String meta = "version=3\ntotalByteSize=" + bloom.getBytes().length + "\nhashCount=" + bloom.getHashCount()
                    + "\nhashType=MURMUR_HASH\ntotalKeyCount=" + bloom.getKeyCount() + "\ntotalMaxKeys="
                    + bloom.getMaxKeys() + "\nnumChunks=1\nchunk0=offset:" + bloomChunkOffset + " onDiskSize:"
                    + bloomChunkSize + "\n";
            write(HFileBlock.serialize(BlockType.GENERAL_BLOOM_META, -1, meta));
        }

        // ---- trailer
        trailer.dataIndexCount = dataBlockIndexWriter.getNumRootEntries();
        trailer.totalUncompressedBytes = totalUncompressedBytes;
        trailer.entryCount = entryCount;
        trailer.firstDataBlockOffset = firstDataBlockOffset;
        trailer.lastDataBlockOffset = lastDataBlockOffset;
        write(trailer.serialize());
        out.close();
    }

    /** The bit array as hex, 32 bytes (= 256 bits) per line. */
    private static String hexLines(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < b.length; i += 32) {
            byte[] line = new byte[Math.min(32, b.length - i)];
            System.arraycopy(b, i, line, 0, line.length);
            sb.append(Bytes.toHex(line)).append('\n');
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "HFileWriter{" + name + ", entries=" + entryCount + "}";
    }
}
