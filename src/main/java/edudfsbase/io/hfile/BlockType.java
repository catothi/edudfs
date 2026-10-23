package edudfsbase.io.hfile;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.BlockType: every block of an HFile begins with
 * an 8-byte "magic" that names its type. The magics are the original ones, so you recognize them
 * in a hex dump of a real HFile.
 *
 * Not in EduDFSBase: LEAF_INDEX ("IDXLEAF2") and INTERMEDIATE_INDEX ("IDXINTE2") of the multi-level
 * index, META ("METABLKc"), DELETE_FAMILY_BLOOM_META ("DFBLMET2"), ENCODED_DATA ("DATABLKE").
 */
public enum BlockType {

    /** Cells, sorted. Section "scanned blocks". */
    DATA("DATABLK*"),
    /** Bit array of the Bloom filter. Lies between the data blocks in the original ("inline"). */
    BLOOM_CHUNK("BLMFBLK2"),
    /** Root level of the data block index: first key + offset of every data block. Load-on-open section. */
    ROOT_INDEX("IDXROOT2"),
    /** FileInfo: key/value metadata (last key, max sequence ID, Bloom type, ...). Load-on-open section. */
    FILE_INFO("FILEINF2"),
    /** Metadata of the Bloom filter (size, hash count, chunk index). Load-on-open section. */
    GENERAL_BLOOM_META("BLMFMET2"),
    /** The fixed-size trailer at the very end; the reader starts here. */
    TRAILER("TRABLK\"$");

    private final String magic;

    BlockType(String magic) {
        this.magic = magic;
    }

    public String getMagic() {
        return magic;
    }

    public static BlockType parse(String magic) throws IOException {
        for (BlockType t : values()) {
            if (t.magic.equals(magic)) {
                return t;
            }
        }
        throw new IOException("Invalid HFile block magic: '" + magic + "'");
    }
}
