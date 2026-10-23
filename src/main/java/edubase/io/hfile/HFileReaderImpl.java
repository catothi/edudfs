package edubase.io.hfile;

import edubase.Cell;
import edubase.CellComparator;
import edubase.KeyValue;
import edubase.codec.KeyValueCodec;
import edubase.io.PositionedReadable;
import edubase.util.BloomFilterChunk;
import edubase.util.Bytes;
import edubase.util.ReadTrace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFileReaderImpl.
 *
 * Opening (as in the original):
 * <ol>
 *   <li>read the trailer at (file length - 512) - it says where the load-on-open section begins,</li>
 *   <li>read the load-on-open section completely: root index, FileInfo, Bloom metadata,</li>
 *   <li>(EduBase:) also load the Bloom chunk right away. The original fetches Bloom chunks on demand
 *       through the BlockCache.</li>
 * </ol>
 * After that the reader knows every block offset and first key. Data blocks are read only when a
 * scanner needs them - first from the BlockCache, otherwise from HDFS.
 */
public class HFileReaderImpl implements HFile.Reader {

    private final String name;
    private final PositionedReadable in;
    private final long fileSize;
    private final LruBlockCache cache;
    private final FixedFileTrailer trailer;
    private final HFileBlockIndex.BlockIndexReader dataBlockIndexReader;
    private final HFileInfo fileInfo;
    private final Map<String, String> bloomMeta = new TreeMap<>();
    private BloomFilterChunk bloom;
    /** All non-data blocks in file order (for HFilePrettyPrinter --layout). */
    private final List<HFileBlock> loadOnOpenBlocks = new ArrayList<>();

    HFileReaderImpl(String name, PositionedReadable in, LruBlockCache cache) throws IOException {
        this.name = name;
        this.in = in;
        this.fileSize = in.getLength();
        this.cache = cache;
        this.trailer = FixedFileTrailer.readFromStream(in, fileSize, name);
        HFileBlock indexBlock = null;
        HFileBlock infoBlock = null;
        long offset = trailer.getLoadOnOpenDataOffset();
        long end = fileSize - FixedFileTrailer.TRAILER_SIZE;
        while (offset < end) {
            HFileBlock b = HFileBlock.read(in, offset, name);
            loadOnOpenBlocks.add(b);
            switch (b.getBlockType()) {
                case ROOT_INDEX -> indexBlock = b;
                case FILE_INFO -> infoBlock = b;
                case GENERAL_BLOOM_META -> {
                    for (String line : b.getLines()) {
                        int i = line.indexOf('=');
                        bloomMeta.put(line.substring(0, i), line.substring(i + 1));
                    }
                }
                default -> throw new IOException("Unexpected block " + b + " in load-on-open section of " + name);
            }
            offset += b.getOnDiskSizeWithHeader();
        }
        if (indexBlock == null || infoBlock == null) {
            throw new IOException("Load-on-open section incomplete in " + name);
        }
        this.dataBlockIndexReader = new HFileBlockIndex.BlockIndexReader(indexBlock);
        this.fileInfo = HFileInfo.parse(infoBlock);
        if (bloomMeta.containsKey("chunk0")) {
            // chunk0=offset:<o> onDiskSize:<n>
            String[] p = bloomMeta.get("chunk0").split(" ");
            long chunkOffset = Long.parseLong(p[0].substring("offset:".length()));
            HFileBlock chunk = HFileBlock.read(in, chunkOffset, name);
            loadOnOpenBlocks.add(0, chunk);
            bloom = new BloomFilterChunk(Bytes.fromHex(String.join("", chunk.getLines())),
                    Integer.parseInt(bloomMeta.get("hashCount")), Long.parseLong(bloomMeta.get("totalKeyCount")),
                    Long.parseLong(bloomMeta.get("totalMaxKeys")));
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long length() {
        return fileSize;
    }

    @Override
    public FixedFileTrailer getTrailer() {
        return trailer;
    }

    @Override
    public HFileInfo getHFileInfo() {
        return fileInfo;
    }

    @Override
    public HFileBlockIndex.BlockIndexReader getDataBlockIndexReader() {
        return dataBlockIndexReader;
    }

    @Override
    public BloomFilterChunk getGeneralBloomFilter() {
        return bloom;
    }

    public Map<String, String> getBloomMeta() {
        return Collections.unmodifiableMap(bloomMeta);
    }

    /** The file itself (for HFilePrettyPrinter, which walks over all blocks). */
    PositionedReadable getInput() {
        return in;
    }

    public List<HFileBlock> getLoadOnOpenBlocks() {
        return loadOnOpenBlocks;
    }

    @Override
    public KeyValue getFirstKey() {
        List<HFileBlockIndex.Entry> e = dataBlockIndexReader.getEntries();
        return e.isEmpty() ? null : e.get(0).getFirstKey();
    }

    @Override
    public KeyValue getLastKey() {
        String k = fileInfo.get(HFileInfo.LASTKEY);
        try {
            return k == null ? null : KeyValueCodec.decodeKey(k);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long getEntries() {
        return trailer.getEntryCount();
    }

    @Override
    public HFileBlock readBlock(long offset, boolean cacheBlock, ReadTrace trace) throws IOException {
        LruBlockCache.BlockCacheKey key = new LruBlockCache.BlockCacheKey(name, offset);
        HFileBlock b = cache == null ? null : cache.getBlock(key);
        if (b != null) {
            if (trace != null) {
                trace.add("      block @" + offset + ": BlockCache HIT");
            }
            return b;
        }
        b = HFileBlock.read(in, offset, name);
        b.getCells();  // parse once, the cache keeps the parsed cells
        if (trace != null) {
            trace.add("      block @" + offset + ": BlockCache " + (cache == null ? "off" : "MISS")
                    + " -> read " + b.getOnDiskSizeWithHeader() + " bytes from HDFS (pread)");
        }
        if (cache != null && cacheBlock) {
            cache.cacheBlock(key, b);
        }
        return b;
    }

    @Override
    public HFileScanner getScanner(boolean cacheBlocks, ReadTrace trace) {
        return new HFileScannerImpl(cacheBlocks, trace);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public String toString() {
        return "HFileReader{" + name + ", entries=" + getEntries() + ", blocks=" + trailer.getDataIndexCount() + "}";
    }

    /** Corresponds to HFileReaderImpl.HFileScannerImpl. */
    private class HFileScannerImpl implements HFileScanner {
        private final boolean cacheBlocks;
        private final ReadTrace trace;
        private int blockIdx = -1;
        private List<KeyValue> cells;
        private int pos;

        HFileScannerImpl(boolean cacheBlocks, ReadTrace trace) {
            this.cacheBlocks = cacheBlocks;
            this.trace = trace;
        }

        private boolean loadBlock(int idx) throws IOException {
            List<HFileBlockIndex.Entry> entries = dataBlockIndexReader.getEntries();
            if (idx >= entries.size()) {
                cells = null;
                return false;
            }
            HFileBlockIndex.Entry e = entries.get(idx);
            if (trace != null) {
                trace.add("      data block #" + idx + " (first key " + KeyValueCodec.encodeKey(e.getFirstKey()) + ")");
            }
            cells = readBlock(e.getOffset(), cacheBlocks, trace).getCells();
            blockIdx = idx;
            return true;
        }

        @Override
        public boolean seekTo() throws IOException {
            if (!loadBlock(0)) {
                return false;
            }
            pos = 0;
            return !cells.isEmpty();
        }

        @Override
        public boolean seekTo(Cell key) throws IOException {
            if (dataBlockIndexReader.getRootBlockCount() == 0) {
                return false;
            }
            int idx = dataBlockIndexReader.rootBlockContainingKey(key);
            if (trace != null) {
                trace.add("      block index: key " + KeyValueCodec.encodeKey(key) + " -> data block #" + idx + " of "
                        + dataBlockIndexReader.getRootBlockCount());
            }
            if (blockIdx != idx || cells == null) {
                loadBlock(idx);
            }
            return positionAtOrAfter(key, 0);
        }

        @Override
        public boolean reseekTo(Cell key) throws IOException {
            if (cells != null && !cells.isEmpty()
                    && CellComparator.getInstance().compare(cells.get(cells.size() - 1), key) >= 0) {
                return positionAtOrAfter(key, pos);  // the key is in the current block
            }
            return seekTo(key);
        }

        private boolean positionAtOrAfter(Cell key, int from) throws IOException {
            int lo = from;
            int hi = cells.size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (CellComparator.getInstance().compare(cells.get(mid), key) < 0) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            pos = lo;
            while (pos >= cells.size()) {
                if (!loadBlock(blockIdx + 1)) {
                    return false;
                }
                pos = 0;
            }
            return true;
        }

        @Override
        public boolean next() throws IOException {
            if (cells == null) {
                return false;
            }
            pos++;
            while (pos >= cells.size()) {
                if (!loadBlock(blockIdx + 1)) {
                    return false;
                }
                pos = 0;
            }
            return true;
        }

        @Override
        public Cell getCell() {
            return cells == null || pos >= cells.size() ? null : cells.get(pos);
        }

        @Override
        public boolean isSeeked() {
            return cells != null;
        }
    }
}
