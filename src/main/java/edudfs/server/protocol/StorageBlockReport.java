package edudfs.server.protocol;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.StorageBlockReport:
 * the full block report consists of one entry PER STORAGE.
 * Even an empty volume is reported - that is the only way the NameNode knows it exists.
 */
public class StorageBlockReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatanodeStorage storage;
    private final BlockReportReplica[] blocks;

    public StorageBlockReport(DatanodeStorage storage, BlockReportReplica[] blocks) {
        this.storage = storage;
        this.blocks = blocks;
    }

    public DatanodeStorage getStorage() {
        return storage;
    }

    public BlockReportReplica[] getBlocks() {
        return blocks;
    }
}
