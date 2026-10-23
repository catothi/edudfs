package edudfsbase.io.hfile;

import edudfsbase.KeyValue;
import edudfsbase.util.BloomFilterChunk;
import edudfsbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * EduDFSBase: all blocks of an HFile in file order with their section and a short description - the
 * basis of {@code ./hbase hfile --layout}, {@code --offset}, {@code --dump} and {@code --html}.
 * A reader never does this (it only follows the trailer and the index); the tools walk the file from
 * offset 0 to the trailer, block header by block header.
 */
public final class HFileBlockMap {

    /** The sections of HFile version 2/3 (HBase reference guide, appendix "HFile format"). */
    public enum Section {
        SCANNED("scanned block section", "data blocks, leaf index blocks, Bloom chunks - a scan reads them in order"),
        NON_SCANNED("non-scanned block section", "intermediate index blocks (and META blocks) - only read via the index"),
        LOAD_ON_OPEN("load-on-open section", "read completely when the file is opened"),
        TRAILER("trailer", "fixed size at the end of the file - the reader starts here");

        public final String title;
        public final String description;

        Section(String title, String description) {
            this.title = title;
            this.description = description;
        }
    }

    /** One block (or the trailer). */
    public static final class Item {
        public final long offset;
        public final int size;
        public final BlockType type;
        public final HFileBlock block;  // null for the trailer
        public final Section section;
        public final String role;
        public String description;
        /** Number of the data block (DATA only), else -1. */
        public final int dataBlockNo;

        Item(long offset, int size, BlockType type, HFileBlock block, Section section, String role, int dataBlockNo) {
            this.offset = offset;
            this.size = size;
            this.type = type;
            this.block = block;
            this.section = section;
            this.role = role;
            this.dataBlockNo = dataBlockNo;
        }

        public long end() {
            return offset + size;
        }
    }

    private final HFileReaderImpl reader;
    private final List<Item> items = new ArrayList<>();
    private final Map<Long, Item> byOffset = new HashMap<>();

    public HFileBlockMap(HFileReaderImpl r) throws IOException {
        this.reader = r;
        FixedFileTrailer t = r.getTrailer();
        long end = r.length() - FixedFileTrailer.TRAILER_SIZE;
        long offset = 0;
        int dataNo = 0;
        boolean dataIndexSeen = false;
        while (offset < end) {
            HFileBlock b = HFileBlock.read(r.getInput(), offset, r.getName());
            BlockType type = b.getBlockType();
            Section section = offset >= t.getLoadOnOpenDataOffset() ? Section.LOAD_ON_OPEN
                    : type == BlockType.INTERMEDIATE_INDEX ? Section.NON_SCANNED : Section.SCANNED;
            String role = switch (type) {
                case DATA -> "data block #" + dataNo;
                case LEAF_INDEX -> "leaf index block";
                case INTERMEDIATE_INDEX -> "intermediate index block";
                case ROOT_INDEX -> dataIndexSeen ? "meta block index" : "root data index";
                case BLOOM_CHUNK -> "Bloom chunk";
                case FILE_INFO -> "FileInfo";
                case GENERAL_BLOOM_META -> "Bloom metadata";
                default -> type.name();
            };
            if (type == BlockType.ROOT_INDEX) {
                dataIndexSeen = true;
            }
            add(new Item(offset, b.getOnDiskSizeWithHeader(), type, b, section, role,
                    type == BlockType.DATA ? dataNo++ : -1));
            offset += b.getOnDiskSizeWithHeader();
        }
        add(new Item(end, FixedFileTrailer.TRAILER_SIZE, BlockType.TRAILER, null, Section.TRAILER, "fixed file trailer",
                -1));
        for (Item i : items) {
            i.description = describe(i);
        }
    }

    private void add(Item i) {
        items.add(i);
        byOffset.put(i.offset, i);
    }

    public List<Item> getItems() {
        return items;
    }

    public Item at(long offset) {
        return byOffset.get(offset);
    }

    /** The block that contains the byte at offset (null if outside the file). */
    public Item containing(long offset) {
        for (Item i : items) {
            if (offset >= i.offset && offset < i.end()) {
                return i;
            }
        }
        return null;
    }

    /** "data block #3", "leaf index block @4711", ... for the block an index entry points to. */
    public String nameOf(long offset) {
        Item i = at(offset);
        if (i == null) {
            return "? @" + offset;
        }
        return i.role + " @" + offset;
    }

    private String describe(Item i) throws IOException {
        HFileBlock b = i.block;
        FixedFileTrailer t = reader.getTrailer();
        switch (i.type) {
            case DATA -> {
                List<KeyValue> cells = b.getCells();
                return i.role + ": " + cells.size() + " cells, " + Bytes.toStringBinary(cells.get(0).getRow()) + " .. "
                        + Bytes.toStringBinary(cells.get(cells.size() - 1).getRow());
            }
            case LEAF_INDEX -> {
                List<HFileBlockIndex.Entry> e = b.getIndexEntries();
                return i.role + ": " + e.size() + " entries -> data blocks " + range(e);
            }
            case INTERMEDIATE_INDEX -> {
                List<HFileBlockIndex.Entry> e = b.getIndexEntries();
                return i.role + ": " + e.size() + " entries -> " + childKind(e) + " index blocks";
            }
            case ROOT_INDEX -> {
                if (i.role.startsWith("meta")) {
                    return i.role + ": " + b.getLines().size() + " entries (no META blocks in EduDFSBase)";
                }
                List<HFileBlockIndex.Entry> e = b.getIndexEntries();
                String target = t.getNumDataIndexLevels() == 1 ? "data blocks " + range(e)
                        : childKind(e) + " index blocks";
                return i.role + ": " + e.size() + " entries -> " + target + "  (index level 1 of "
                        + t.getNumDataIndexLevels() + ")";
            }
            case BLOOM_CHUNK -> {
                BloomFilterChunk bloom = reader.getGeneralBloomFilter();
                return "Bloom bit array: " + bloom.getBitSize() + " bits, " + bloom.getKeyCount() + " keys, "
                        + String.format(Locale.ROOT, "%.0f%% of bits set", 100 * bloom.fillRatio());
            }
            case FILE_INFO -> {
                return "FileInfo: " + b.getLines().size() + " key/value pairs (LASTKEY, MAX_SEQ_ID_KEY, ...)";
            }
            case GENERAL_BLOOM_META -> {
                return "Bloom metadata: hash count, key count, where the bit array is";
            }
            case TRAILER -> {
                return "fixed trailer: loadOnOpenDataOffset=" + t.getLoadOnOpenDataOffset() + ", " + t.getEntryCount()
                        + " cells, version 3.3  <- the reader starts here";
            }
            default -> {
                return i.role;
            }
        }
    }

    private String childKind(List<HFileBlockIndex.Entry> e) {
        Item child = e.isEmpty() ? null : at(e.get(0).getOffset());
        return child == null ? "?" : child.type == BlockType.LEAF_INDEX ? "leaf" : "intermediate";
    }

    private String range(List<HFileBlockIndex.Entry> e) {
        if (e.isEmpty()) {
            return "-";
        }
        Item first = at(e.get(0).getOffset());
        Item last = at(e.get(e.size() - 1).getOffset());
        if (first == null || last == null) {
            return "?";
        }
        return first.dataBlockNo == last.dataBlockNo ? "#" + first.dataBlockNo
                : "#" + first.dataBlockNo + ".." + "#" + last.dataBlockNo;
    }
}
