package edudfs.protocol;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.LastBlockWithStatus:
 * response to append() - file status plus the file's current last block.
 * The client needs the last block as "previous" for its first addBlock().
 */
public class LastBlockWithStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    private final LocatedBlock lastBlock;
    private final HdfsFileStatus fileStatus;

    public LastBlockWithStatus(LocatedBlock lastBlock, HdfsFileStatus fileStatus) {
        this.lastBlock = lastBlock;
        this.fileStatus = fileStatus;
    }

    /** Last block of the file, or null if the file is empty. */
    public LocatedBlock getLastBlock() {
        return lastBlock;
    }

    public HdfsFileStatus getFileStatus() {
        return fileStatus;
    }
}
