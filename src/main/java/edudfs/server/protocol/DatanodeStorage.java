package edudfs.server.protocol;

import edudfs.protocol.StorageType;

import java.io.Serializable;
import java.util.UUID;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.protocol.DatanodeStorage:
 * identity of a storage (= a volume / a disk) of a DataNode, as it is
 * sent to the NameNode in block reports, IBRs and heartbeats.
 */
public class DatanodeStorage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** State of a storage. READ_ONLY_SHARED exists in the original for shared storage. */
    public enum State {
        NORMAL,
        READ_ONLY_SHARED,
        FAILED
    }

    private final String storageID;
    private final State state;
    private final StorageType storageType;

    public DatanodeStorage(String storageID, State state, StorageType storageType) {
        this.storageID = storageID;
        this.state = state;
        this.storageType = storageType;
    }

    public DatanodeStorage(String storageID) {
        this(storageID, State.NORMAL, StorageType.DEFAULT);
    }

    /** As in the original: "DS-" + random UUID, generated once when a volume is formatted. */
    public static String generateUuid() {
        return "DS-" + UUID.randomUUID();
    }

    public String getStorageID() {
        return storageID;
    }

    public State getState() {
        return state;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    @Override
    public String toString() {
        return "[" + storageType + "]" + storageID + ":" + state;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof DatanodeStorage && storageID.equals(((DatanodeStorage) o).storageID));
    }

    @Override
    public int hashCode() {
        return storageID.hashCode();
    }
}
