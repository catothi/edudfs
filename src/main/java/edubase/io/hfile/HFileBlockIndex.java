package edubase.io.hfile;

import edubase.Cell;
import edubase.CellComparator;
import edubase.KeyValue;
import edubase.codec.KeyValueCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFileBlockIndex: the data block index.
 *
 * For every data block one entry: offset, size and FIRST key of the block. Because the keys in
 * the file are sorted, a binary search over the entries yields the only block that can contain
 * a key - the reader then loads exactly this block instead of the whole file.
 * <pre>
 *   IDXROOT2 ...
 *   offset=0 onDiskSize=1182 firstKey=row00/cf:name/1761213640123/Put
 *   offset=1182 onDiskSize=1176 firstKey=row12/cf:age/1761213640456/Put
 * </pre>
 * Simplification: one level ("root") only. The original builds a multi-level index for large
 * files: leaf index blocks (IDXLEAF2) between the data blocks, above them intermediate and root
 * levels, so that the root level stays small enough to be kept in memory.
 */
public final class HFileBlockIndex {

    private HFileBlockIndex() {
    }

    /** One entry of the root index. */
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
    }

    /** Collects the entries while writing (BlockIndexWriter in the original). */
    static final class BlockIndexWriter {
        private final List<Entry> entries = new ArrayList<>();

        void addEntry(KeyValue firstKey, long offset, int onDiskSize) {
            entries.add(new Entry(offset, onDiskSize, firstKey));
        }

        int getNumRootEntries() {
            return entries.size();
        }

        String toBody() {
            StringBuilder sb = new StringBuilder();
            for (Entry e : entries) {
                sb.append(e.toLine()).append('\n');
            }
            return sb.toString();
        }
    }

    /** The index in memory after opening (BlockIndexReader / CellBasedKeyBlockIndexReader in the original). */
    public static final class BlockIndexReader {
        private final List<Entry> entries;

        BlockIndexReader(HFileBlock rootBlock) throws IOException {
            List<Entry> r = new ArrayList<>();
            for (String line : rootBlock.getLines()) {
                String[] p = line.split(" ", 3);
                r.add(new Entry(Long.parseLong(p[0].substring("offset=".length())),
                        Integer.parseInt(p[1].substring("onDiskSize=".length())),
                        KeyValueCodec.decodeKey(p[2].substring("firstKey=".length()))));
            }
            this.entries = Collections.unmodifiableList(r);
        }

        public List<Entry> getEntries() {
            return entries;
        }

        public int getRootBlockCount() {
            return entries.size();
        }

        /**
         * Binary search: index of the block in which the first cell &gt;= key can begin, i.e. the
         * last block whose first key is SMALLER than key (block 0 if there is none).
         */
        public int rootBlockContainingKey(Cell key) {
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
    }
}
