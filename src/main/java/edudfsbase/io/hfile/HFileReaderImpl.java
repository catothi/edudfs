package edudfsbase.io.hfile;

import edudfsbase.Cell;
import edudfsbase.CellComparator;
import edudfsbase.KeyValue;
import edudfsbase.codec.KeyValueCodec;
import edudfsbase.io.PositionedReadable;
import edudfsbase.util.BloomFilterChunk;
import edudfsbase.util.Bytes;
import edudfsbase.util.ReadTrace;

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
 *   <li>read the load-on-open section completely: root data index, meta index, FileInfo, Bloom metadata,</li>
 *   <li>(EduDFSBase:) also load the Bloom chunk right away. The original fetches Bloom chunks on demand
 *       through the BlockCache.</li>
 * </ol>
 * After that only the ROOT of the block index is in memory. A seek walks down the tree: root -&gt;
 * (intermediate -&gt;) (leaf -&gt;) data block; index and data blocks are read when needed - first from the
 * BlockCache, otherwise from HDFS. A scan moves on block by block in file order and skips the leaf
 * index blocks between the data blocks.
 */
public class HFileReaderImpl implements HFile.Reader {

    private final String name;
    private final PositionedReadable in;
    private final long fileSize;
    private final LruBlockCache cache;
    private final FixedFileTrailer trailer;
    private final HFileBlockIndex.BlockIndexReader dataBlockIndexReader;
    private final HFileInfo fileInfo;
    private int metaIndexEntries;
    private List<HFileBlockIndex.Entry> dataBlockEntries;
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
                case ROOT_INDEX -> {
                    if (indexBlock == null) {
                        indexBlock = b;  // the first root index is the data index ...
                    } else {
                        metaIndexEntries = b.getLines().size();  // ... the second one the meta index
                    }
                }
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
        this.dataBlockIndexReader = new HFileBlockIndex.BlockIndexReader(indexBlock, trailer.getNumDataIndexLevels());
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

    public int getMetaIndexEntries() {
        return metaIndexEntries;
    }

    @Override
    public KeyValue getFirstKey() {
        List<HFileBlockIndex.Entry> e = dataBlockIndexReader.getRootEntries();
        return e.isEmpty() ? null : e.get(0).getFirstKey();
    }

    /**
     * Walks down the block index (CellBasedKeyBlockIndexReader.loadDataBlockWithScanInfo): binary search in
     * the root, then in every index block of the next level, until an entry names a data block.
     */
    HFileBlockIndex.Entry seekToDataBlock(Cell key, boolean cacheBlocks, ReadTrace trace) throws IOException {
        List<HFileBlockIndex.Entry> entries = dataBlockIndexReader.getRootEntries();
        int levels = dataBlockIndexReader.getNumLevels();
        String where = "root IDXROOT2 (in memory, " + entries.size() + " entries)";
        for (int level = 1; ; level++) {
            int i = HFileBlockIndex.binarySearch(entries, key);
            HFileBlockIndex.Entry e = entries.get(i);
            boolean dataBlock = level == levels;
            if (trace != null) {
                trace.add("      block index level " + level + "/" + levels + ", " + where + ": key "
                        + KeyValueCodec.encodeKey(key) + " -> entry #" + i + " (first key "
                        + KeyValueCodec.encodeKey(e.getFirstKey()) + ") -> " + (dataBlock ? "data block"
                        : level + 1 == levels ? "leaf index block" : "intermediate index block") + " @"
                        + e.getOffset());
            }
            if (dataBlock) {
                return e;
            }
            HFileBlock b = readBlock(e.getOffset(), cacheBlocks, trace);
            entries = b.getIndexEntries();
            where = b.getBlockType().getMagic() + " @" + b.getOffset() + " (" + entries.size() + " entries)";
        }
    }

    /** The next data block after b in file order; index and Bloom blocks in between are skipped. */
    HFileBlock readNextDataBlock(HFileBlock b, boolean cacheBlocks, ReadTrace trace) throws IOException {
        long offset = b.getOffset() + b.getOnDiskSizeWithHeader();
        while (offset <= trailer.getLastDataBlockOffset()) {
            HFileBlock next = readBlock(offset, cacheBlocks, trace);
            if (next.getBlockType() == BlockType.DATA) {
                return next;
            }
            if (trace != null) {
                trace.add("      skip " + next.getBlockType().getMagic() + " @" + offset
                        + " (index block between the data blocks)");
            }
            offset += next.getOnDiskSizeWithHeader();
        }
        return null;
    }

    /** All data block entries, read from the leaves of the index (for tools; a reader never needs them all). */
    public synchronized List<HFileBlockIndex.Entry> getDataBlockEntries() throws IOException {
        if (dataBlockEntries == null) {
            List<HFileBlockIndex.Entry> r = new ArrayList<>();
            collect(dataBlockIndexReader.getRootEntries(), 1, r);
            dataBlockEntries = Collections.unmodifiableList(r);
        }
        return dataBlockEntries;
    }

    private void collect(List<HFileBlockIndex.Entry> entries, int level, List<HFileBlockIndex.Entry> out)
            throws IOException {
        if (level == dataBlockIndexReader.getNumLevels()) {
            out.addAll(entries);
            return;
        }
        for (HFileBlockIndex.Entry e : entries) {
            collect(readBlock(e.getOffset(), false, null).getIndexEntries(), level + 1, out);
        }
    }

    /** Index block at offset (for tools that draw the tree). */
    public HFileBlock readIndexBlock(long offset) throws IOException {
        return readBlock(offset, false, null);
    }

    @Override
    public int getDataBlockCount() throws IOException {
        return getDataBlockEntries().size();
    }

    /**
     * Corresponds to midKey(): first key of the middle data block - where a region would be split. With a
     * multi-level index the root names the leaf block and the entry in it; otherwise the middle root entry.
     */
    public KeyValue midKey() throws IOException {
        List<HFileBlockIndex.Entry> root = dataBlockIndexReader.getRootEntries();
        if (root.isEmpty()) {
            return null;
        }
        if (dataBlockIndexReader.getMidLeafBlockOffset() < 0) {
            return root.get((root.size() - 1) / 2).getFirstKey();
        }
        return readBlock(dataBlockIndexReader.getMidLeafBlockOffset(), false, null).getIndexEntries()
                .get(dataBlockIndexReader.getMidKeyEntry()).getFirstKey();
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
        if (b.getBlockType() == BlockType.DATA) {
            b.getCells();  // parse once, the cache keeps the parsed cells
        }
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
        return "HFileReader{" + name + ", entries=" + getEntries() + ", indexLevels=" + trailer.getNumDataIndexLevels() + "}";
    }

    /** Corresponds to HFileReaderImpl.HFileScannerImpl. */
    private class HFileScannerImpl implements HFileScanner {
        private final boolean cacheBlocks;
        private final ReadTrace trace;
        private HFileBlock curBlock;
        private List<KeyValue> cells;
        private int pos;

        HFileScannerImpl(boolean cacheBlocks, ReadTrace trace) {
            this.cacheBlocks = cacheBlocks;
            this.trace = trace;
        }

        private void setBlock(HFileBlock b) throws IOException {
            curBlock = b;
            cells = b == null ? null : b.getCells();
            if (b != null && trace != null) {
                trace.add("      data block @" + b.getOffset() + " (first key "
                        + KeyValueCodec.encodeKey(cells.get(0)) + ")");
            }
        }

        private boolean nextBlock() throws IOException {
            setBlock(readNextDataBlock(curBlock, cacheBlocks, trace));
            return cells != null;
        }

        @Override
        public boolean seekTo() throws IOException {
            if (trailer.getFirstDataBlockOffset() < 0) {
                return false;
            }
            setBlock(readBlock(trailer.getFirstDataBlockOffset(), cacheBlocks, trace));
            pos = 0;
            return !cells.isEmpty();
        }

        @Override
        public boolean seekTo(Cell key) throws IOException {
            if (dataBlockIndexReader.getRootBlockCount() == 0) {
                return false;
            }
            HFileBlockIndex.Entry e = seekToDataBlock(key, cacheBlocks, trace);
            if (curBlock == null || cells == null || curBlock.getOffset() != e.getOffset()) {
                setBlock(readBlock(e.getOffset(), cacheBlocks, trace));
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
                if (!nextBlock()) {
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
                if (!nextBlock()) {
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
