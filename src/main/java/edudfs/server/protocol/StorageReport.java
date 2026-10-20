package edudfs.server.protocol;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.StorageReport:
 * part of every heartbeat - capacity and usage of ONE storage.
 * The NameNode uses "remaining" when choosing targets (BlockPlacementPolicy).
 * (The original additionally has: nonDfsUsed, blockPoolUsed.)
 */
public class StorageReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatanodeStorage storage;
    private final boolean failed;
    private final long capacity;
    private final long dfsUsed;
    private final long remaining;

    public StorageReport(DatanodeStorage storage, boolean failed, long capacity, long dfsUsed, long remaining) {
        this.storage = storage;
        this.failed = failed;
        this.capacity = capacity;
        this.dfsUsed = dfsUsed;
        this.remaining = remaining;
    }

    public DatanodeStorage getStorage() {
        return storage;
    }

    public boolean isFailed() {
        return failed;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getDfsUsed() {
        return dfsUsed;
    }

    public long getRemaining() {
        return remaining;
    }
}
