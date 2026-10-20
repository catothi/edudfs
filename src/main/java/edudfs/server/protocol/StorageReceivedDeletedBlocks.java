package edudfs.server.protocol;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks:
 * incremental block report (IBR), grouped by the storage that holds the replicas.
 */
public class StorageReceivedDeletedBlocks implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatanodeStorage storage;
    private final ReceivedDeletedBlockInfo[] blocks;

    public StorageReceivedDeletedBlocks(DatanodeStorage storage, ReceivedDeletedBlockInfo[] blocks) {
        this.storage = storage;
        this.blocks = blocks;
    }

    public DatanodeStorage getStorage() {
        return storage;
    }

    public ReceivedDeletedBlockInfo[] getBlocks() {
        return blocks;
    }
}
