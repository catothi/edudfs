package edudfs.protocol;

import java.io.Serializable;
import java.util.List;

/** Corresponds to org.apache.hadoop.hdfs.protocol.LocatedBlocks. */
public class LocatedBlocks implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Readable length (excluding the last, still open block of a file being written). */
    private final long fileLength;
    private final boolean underConstruction;
    private final List<LocatedBlock> blocks;

    public LocatedBlocks(long fileLength, boolean underConstruction, List<LocatedBlock> blocks) {
        this.fileLength = fileLength;
        this.underConstruction = underConstruction;
        this.blocks = blocks;
    }

    public long getFileLength() {
        return fileLength;
    }

    public boolean isUnderConstruction() {
        return underConstruction;
    }

    public List<LocatedBlock> getLocatedBlocks() {
        return blocks;
    }
}
