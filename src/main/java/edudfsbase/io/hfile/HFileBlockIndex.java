package edudfsbase.io.hfile;

import edudfsbase.Cell;
import edudfsbase.CellComparator;
import edudfsbase.KeyValue;
import edudfsbase.codec.KeyValueCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFileBlockIndex: the data block index, a B+ tree
 * over the data blocks.
 *
 * Every index entry = offset, size and FIRST key of the block it points to. Because the keys in the
 * file are sorted, a binary search over the entries of one index block yields the only child that can
 * contain a key.
 * <pre>
 *                        IDXROOT2  (load-on-open section, always in memory)
 *                       /        \
 *              IDXINTE2            IDXINTE2       (non-scanned block section, only for large files)
 *             /    |    \         /    |    \
 *        IDXLEAF2 IDXLEAF2 ...                    (between the data blocks, "inline")
 *        /  |  \
 *   DATABLK* DATABLK* ...
 * </pre>
 * Levels grow from the bottom: the writer collects the entries of the data blocks in the current
 * leaf chunk. Once the chunk reaches hfile.index.block.max.size (and has at least
 * hfile.index.block.min.entries entries), it is written as a leaf index block right after the data
 * block just written, and the leaf's first key goes one level up. At close the upper level is written
 * as intermediate blocks as long as it is too large for one root block; the remaining level is the
 * root. A small file never gets a leaf block: its entries go directly into the root (1 level).
 * <pre>
 *   root block (IDXROOT2) body:                     non-root block (IDXLEAF2, IDXINTE2) body:
 *     offset=0 onDiskSize=1182 firstKey=row0000/…     numEntries=7
 *     offset=7300 onDiskSize=640 firstKey=row0084/…   offset=0 onDiskSize=1182 firstKey=row0000/…
 *     midLeafBlockOffset=… midLeafBlockOnDiskSize=… midKeyEntry=…   (only with &gt; 1 level)
 * </pre>
 * Original (binary): root entries = long offset, int size, length-prefixed key; non-root blocks
 * additionally hold a "secondary index" (byte position of every entry) so the binary search can jump
 * into the block without parsing it.
 */
public final class HFileBlockIndex {

    static final String NUM_ENTRIES = "numEntries=";
    static final String MID_KEY = "midLeafBlockOffset=";

    private HFileBlockIndex() {
    }

    /** One index entry: the child block (data or index block) and its first key. */
    public static final class Entry {
        final long offset;
        final int onDiskSize;
        final KeyValue firstKey;

        Entry(long offset, int onDiskSize, KeyValue firstKey) {
            this.offset = offset;
            this.onDiskSize = onDiskSize;
            this.firstKey = firstKey;
        }

        public long getOffset() {
            return offset;
        }

        public int getOnDiskSize() {
            return onDiskSize;
        }

        public KeyValue getFirstKey() {
            return firstKey;
        }

        String toLine() {
            return "offset=" + offset + " onDiskSize=" + onDiskSize + " firstKey=" + KeyValueCodec.encodeKey(firstKey);
        }

        static Entry parse(String line) throws IOException {
            String[] p = line.split(" ", 3);
            return new Entry(Long.parseLong(p[0].substring("offset=".length())),
                    Integer.parseInt(p[1].substring("onDiskSize=".length())),
                    KeyValueCodec.decodeKey(p[2].substring("firstKey=".length())));
        }
    }

    /** Entries of an index block (root or non-root). The mid-key line of the root is skipped. */
    static List<Entry> parseEntries(HFileBlock block) throws IOException {
        List<Entry> r = new ArrayList<>();
        for (String line : block.getLines()) {
            if (line.startsWith("offset=")) {
                r.add(Entry.parse(line));
            }
        }
        return Collections.unmodifiableList(r);
    }

    /**
     * Binary search: the last entry whose first key is SMALLER than key (entry 0 if there is none).
     * Only this child can contain the first cell &gt;= key.
     */
    public static int binarySearch(List<Entry> entries, Cell key) {
        int lo = 0;
        int hi = entries.size() - 1;
        int result = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (CellComparator.getInstance().compareKeys(entries.get(mid).firstKey, key) < 0) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    /** Corresponds to HFileBlockIndex.BlockIndexChunk: the entries of one index block being built. */
    static final class BlockIndexChunk {
        private final List<Entry> entries = new ArrayList<>();
        /** For the mid-key: number of data blocks below each entry, cumulative. */
        private final List<Long> numSubEntriesAt = new ArrayList<>();
        private int size;

        void add(Entry e, long curTotalNumSubEntries) {
            entries.add(e);
            numSubEntriesAt.add(curTotalNumSubEntries);
            size += e.toLine().length() + 1;
        }

        int getNumEntries() {
            return entries.size();
        }

        /** Bytes of the entries - compared with hfile.index.block.max.size. */
        int getSize() {
            return size;
        }

        String toNonRootBody() {
            StringBuilder sb = new StringBuilder(NUM_ENTRIES).append(entries.size()).append('\n');
            for (Entry e : entries) {
                sb.append(e.toLine()).append('\n');
            }
            return sb.toString();
        }

        String toRootBody(String midKeyLine) {
            StringBuilder sb = new StringBuilder();
            for (Entry e : entries) {
                sb.append(e.toLine()).append('\n');
            }
            if (midKeyLine != null) {
                sb.append(midKeyLine).append('\n');
            }
            return sb.toString();
        }
    }

    /** Where the writer puts blocks (the HFile output stream and its position). */
    interface BlockSink {
        long getPos();

        void write(byte[] block) throws IOException;
    }

    /** Corresponds to HFileBlockIndex.BlockIndexWriter: builds the index while the data blocks are written. */
    static final class BlockIndexWriter {
        private final int maxChunkSize;
        private final int minIndexNumEntries;
        /** Entries of the data blocks not yet written as a leaf block. */
        private BlockIndexChunk curInlineChunk = new BlockIndexChunk();
        /** Level above the leaves (later: the root). */
        private BlockIndexChunk rootChunk = new BlockIndexChunk();
        private int numLevels = 1;
        private long totalNumDataBlocks;
        /** Leaf blocks written so far: offset, size, number of entries (for the mid-key). */
        private final List<long[]> leaves = new ArrayList<>();
        private long prevLeafOffset = -1;
        private long prevIntermediateOffset = -1;
        /** Sum of the sizes of all index blocks (trailer: uncompressedDataIndexSize). */
        private long totalBlockSize;

        BlockIndexWriter(int maxChunkSize, int minIndexNumEntries) {
            this.maxChunkSize = maxChunkSize;
            this.minIndexNumEntries = Math.max(2, minIndexNumEntries);
        }

        /** A data block was written: its entry goes into the current leaf chunk. */
        void addEntry(KeyValue firstKey, long offset, int onDiskSize) {
            totalNumDataBlocks++;
            curInlineChunk.add(new Entry(offset, onDiskSize, firstKey), totalNumDataBlocks);
        }

        private boolean isFull(BlockIndexChunk chunk) {
            return chunk.getSize() >= maxChunkSize && chunk.getNumEntries() >= minIndexNumEntries;
        }

        /**
         * Should the current leaf chunk be written now (InlineBlockWriter.shouldWriteBlock)? At close: if no
         * leaf block has been written yet, the entries become the root instead (single-level index).
         */
        boolean shouldWriteBlock(boolean closing) {
            if (curInlineChunk.getNumEntries() == 0) {
                return false;
            }
            if (closing) {
                if (leaves.isEmpty()) {
                    rootChunk = curInlineChunk;
                    curInlineChunk = new BlockIndexChunk();
                    return false;
                }
                return true;
            }
            return isFull(curInlineChunk);
        }

        /** Writes the current leaf chunk as IDXLEAF2 at the sink's position; its first key goes one level up. */
        void writeInlineBlock(BlockSink sink) throws IOException {
            long offset = sink.getPos();
            byte[] b = HFileBlock.serialize(BlockType.LEAF_INDEX, prevLeafOffset, curInlineChunk.toNonRootBody());
            sink.write(b);
            totalBlockSize += b.length;
            prevLeafOffset = offset;
            numLevels = 2;
            leaves.add(new long[]{offset, b.length, curInlineChunk.getNumEntries()});
            rootChunk.add(new Entry(offset, b.length, curInlineChunk.entries.get(0).firstKey), totalNumDataBlocks);
            curInlineChunk = new BlockIndexChunk();
        }

        /**
         * At close: intermediate levels as long as the upper level does not fit into one root block, then the
         * root. Returns the offset of the root block (= start of the load-on-open section).
         */
        long writeIndexBlocks(BlockSink sink) throws IOException {
            while (rootChunk.getSize() > maxChunkSize && rootChunk.getNumEntries() > minIndexNumEntries
                    && numLevels < 16) {
                rootChunk = writeIntermediateLevel(sink, rootChunk);
                numLevels++;
            }
            long rootOffset = sink.getPos();
            byte[] b = HFileBlock.serialize(BlockType.ROOT_INDEX, -1, rootChunk.toRootBody(midKeyLine()));
            sink.write(b);
            totalBlockSize += b.length;
            return rootOffset;
        }

        private BlockIndexChunk writeIntermediateLevel(BlockSink sink, BlockIndexChunk level) throws IOException {
            BlockIndexChunk parent = new BlockIndexChunk();
            BlockIndexChunk cur = new BlockIndexChunk();
            for (int i = 0; i < level.getNumEntries(); i++) {
                cur.add(level.entries.get(i), level.numSubEntriesAt.get(i));
                if (isFull(cur)) {
                    writeIntermediateBlock(sink, parent, cur);
                    cur = new BlockIndexChunk();
                }
            }
            if (cur.getNumEntries() > 0) {
                writeIntermediateBlock(sink, parent, cur);
            }
            return parent;
        }

        private void writeIntermediateBlock(BlockSink sink, BlockIndexChunk parent, BlockIndexChunk chunk)
                throws IOException {
            long offset = sink.getPos();
            byte[] b = HFileBlock.serialize(BlockType.INTERMEDIATE_INDEX, prevIntermediateOffset, chunk.toNonRootBody());
            sink.write(b);
            totalBlockSize += b.length;
            prevIntermediateOffset = offset;
            parent.add(new Entry(offset, b.length, chunk.entries.get(0).firstKey),
                    chunk.numSubEntriesAt.get(chunk.getNumEntries() - 1));
        }

        /**
         * Corresponds to BlockIndexChunk.getMidKeyMetadata: the leaf block that holds the middle data block and
         * the position in it. The first key of that data block is the "mid-key" where a region is split.
         * Single-level index: none (the reader takes the middle root entry).
         */
        private String midKeyLine() {
            if (leaves.isEmpty()) {
                return null;
            }
            long mid = (totalNumDataBlocks - 1) / 2;
            long before = 0;
            for (long[] leaf : leaves) {
                if (mid < before + leaf[2]) {
                    return MID_KEY + leaf[0] + " midLeafBlockOnDiskSize=" + leaf[1] + " midKeyEntry=" + (mid - before);
                }
                before += leaf[2];
            }
            throw new IllegalStateException("mid-key not found");
        }

        int getNumLevels() {
            return numLevels;
        }

        int getNumRootEntries() {
            return rootChunk.getNumEntries();
        }

        long getTotalUncompressedSize() {
            return totalBlockSize;
        }
    }

    /** The index after opening (CellBasedKeyBlockIndexReader in the original): only the root is in memory. */
    public static final class BlockIndexReader {
        private final List<Entry> rootEntries;
        private final int numLevels;
        private final long midLeafBlockOffset;
        private final int midLeafBlockOnDiskSize;
        private final int midKeyEntry;

        BlockIndexReader(HFileBlock rootBlock, int numLevels) throws IOException {
            this.rootEntries = parseEntries(rootBlock);
            this.numLevels = numLevels;
            long off = -1;
            int size = -1;
            int entry = -1;
            for (String line : rootBlock.getLines()) {
                if (line.startsWith(MID_KEY)) {
                    String[] p = line.split(" ");
                    off = Long.parseLong(p[0].substring(p[0].indexOf('=') + 1));
                    size = Integer.parseInt(p[1].substring(p[1].indexOf('=') + 1));
                    entry = Integer.parseInt(p[2].substring(p[2].indexOf('=') + 1));
                }
            }
            this.midLeafBlockOffset = off;
            this.midLeafBlockOnDiskSize = size;
            this.midKeyEntry = entry;
        }

        /** The entries of the root block: data blocks (1 level) or index blocks of the next level. */
        public List<Entry> getRootEntries() {
            return rootEntries;
        }

        public int getRootBlockCount() {
            return rootEntries.size();
        }

        /** 1 = the root points directly to data blocks; 2 = root -&gt; leaf -&gt; data; 3 = root -&gt; intermediate -&gt; ... */
        public int getNumLevels() {
            return numLevels;
        }

        public int rootBlockContainingKey(Cell key) {
            return binarySearch(rootEntries, key);
        }

        /** Leaf block with the mid-key, -1 for a single-level index. */
        public long getMidLeafBlockOffset() {
            return midLeafBlockOffset;
        }

        public int getMidLeafBlockOnDiskSize() {
            return midLeafBlockOnDiskSize;
        }

        public int getMidKeyEntry() {
            return midKeyEntry;
        }
    }
}
