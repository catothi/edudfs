package edubase.io.hfile;

import edubase.KeyValue;
import edubase.codec.KeyValueCodec;
import edubase.io.PositionedReadable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFileBlock: the unit in which an HFile is read
 * (and cached in the BlockCache).
 *
 * Every block = header of FIXED size + body. Because the header always has the same length, the
 * reader can first read the header and learn from it how large the body is - exactly as in the
 * original (there: 33 bytes binary).
 * <pre>
 * EduBase (text, 89 bytes):
 *   DATABLK* onDiskSizeWithoutHeader=      1093 prevBlockOffset=        -1 checksum=5f1d3a2b
 *   K: row01/cf:name/1761213640123/Put/vlen=5/seqid=3 V: Alice
 *   ...
 * Original (binary, 33 bytes):
 *   magic(8) | onDiskSizeWithoutHeader(4) | uncompressedSizeWithoutHeader(4) | prevBlockOffset(8)
 *   | checksumType(1) | bytesPerChecksum(4) | onDiskDataSizeWithHeader(4)
 *   followed by the (possibly compressed/encoded) data and CRC32C checksums per 16 KB.
 * </pre>
 * The checksum here is a CRC32 over the whole body. As in the original, HBase verifies its own
 * checksums when reading (hbase.regionserver.checksum.verify) - in addition to the HDFS CRCs.
 */
public class HFileBlock {

    /** Length of the header line including '\n'. */
    public static final int HEADER_SIZE = header(BlockType.DATA, 0, 0, 0).length;

    private final BlockType blockType;
    private final long offset;
    private final int onDiskSizeWithoutHeader;
    private final long prevBlockOffset;
    private final String body;
    private List<KeyValue> cells;

    HFileBlock(BlockType blockType, long offset, int onDiskSizeWithoutHeader, long prevBlockOffset, String body) {
        this.blockType = blockType;
        this.offset = offset;
        this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
        this.prevBlockOffset = prevBlockOffset;
        this.body = body;
    }

    static byte[] header(BlockType type, int onDiskSizeWithoutHeader, long prevBlockOffset, long checksum) {
        return String.format("%s onDiskSizeWithoutHeader=%10d prevBlockOffset=%10d checksum=%08x\n",
                type.getMagic(), onDiskSizeWithoutHeader, prevBlockOffset, checksum).getBytes(StandardCharsets.US_ASCII);
    }

    /** Header + body as written to the file. The body must be ASCII and end with '\n'. */
    static byte[] serialize(BlockType type, long prevBlockOffset, String body) {
        byte[] b = body.getBytes(StandardCharsets.US_ASCII);
        byte[] h = header(type, b.length, prevBlockOffset, crc(b));
        byte[] all = new byte[h.length + b.length];
        System.arraycopy(h, 0, all, 0, h.length);
        System.arraycopy(b, 0, all, h.length, b.length);
        return all;
    }

    private static long crc(byte[] b) {
        CRC32 c = new CRC32();
        c.update(b);
        return c.getValue();
    }

    /** Reads the block at offset: header first, then the body whose size the header names. */
    static HFileBlock read(PositionedReadable in, long offset, String fileName) throws IOException {
        byte[] h = new byte[HEADER_SIZE];
        in.readFully(offset, h, 0, HEADER_SIZE);
        String header = new String(h, StandardCharsets.US_ASCII);
        BlockType type = BlockType.parse(header.substring(0, 8));
        int size;
        long prev;
        long expectedCrc;
        try {
            size = Integer.parseInt(field(header, "onDiskSizeWithoutHeader="));
            prev = Long.parseLong(field(header, "prevBlockOffset="));
            expectedCrc = Long.parseLong(field(header, "checksum="), 16);
        } catch (RuntimeException e) {
            throw new IOException("Corrupt block header at offset " + offset + " of " + fileName + ": " + header.trim());
        }
        byte[] b = new byte[size];
        in.readFully(offset + HEADER_SIZE, b, 0, size);
        if (crc(b) != expectedCrc) {
            throw new IOException("Checksum failed for " + type + " block at offset " + offset + " of " + fileName
                    + " - the file is corrupt");
        }
        return new HFileBlock(type, offset, size, prev, new String(b, StandardCharsets.US_ASCII));
    }

    private static String field(String header, String name) {
        int i = header.indexOf(name) + name.length();
        while (header.charAt(i) == ' ') {
            i++;
        }
        int end = header.indexOf(' ', i);
        return header.substring(i, end < 0 ? header.length() - 1 : end);
    }

    public BlockType getBlockType() {
        return blockType;
    }

    public long getOffset() {
        return offset;
    }

    public int getOnDiskSizeWithHeader() {
        return HEADER_SIZE + onDiskSizeWithoutHeader;
    }

    public int getOnDiskSizeWithoutHeader() {
        return onDiskSizeWithoutHeader;
    }

    public long getPrevBlockOffset() {
        return prevBlockOffset;
    }

    public String getBody() {
        return body;
    }

    public List<String> getLines() {
        List<String> r = new ArrayList<>();
        for (String l : body.split("\n")) {
            if (!l.isEmpty()) {
                r.add(l);
            }
        }
        return r;
    }

    /** For DATA blocks: the cells (parsed once, then cached together with the block). */
    public synchronized List<KeyValue> getCells() throws IOException {
        if (cells == null) {
            List<KeyValue> r = new ArrayList<>();
            for (String l : getLines()) {
                r.add(KeyValueCodec.decode(l));
            }
            cells = Collections.unmodifiableList(r);
        }
        return cells;
    }

    @Override
    public String toString() {
        return blockType.getMagic() + " @" + offset + " (" + getOnDiskSizeWithHeader() + " bytes)";
    }
}
