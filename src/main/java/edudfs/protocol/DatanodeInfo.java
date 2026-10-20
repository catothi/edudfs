package edudfs.protocol;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.DatanodeInfo:
 * DatanodeID + status as clients see it (e.g. in LocatedBlock).
 */
public class DatanodeInfo extends DatanodeID {

    private static final long serialVersionUID = 1L;

    private long lastUpdate;      // wall-clock time of the last heartbeat
    private long lastUpdateMonotonic;
    private long capacity;
    private long dfsUsed;
    private long remaining;
    private int numBlocks;
    private boolean alive;

    public DatanodeInfo(DatanodeID id) {
        super(id);
    }

    /** Copy (used, among other things, to avoid serializing server-internal objects). */
    public DatanodeInfo(DatanodeInfo from) {
        super(from);
        this.lastUpdate = from.lastUpdate;
        this.lastUpdateMonotonic = from.lastUpdateMonotonic;
        this.capacity = from.getCapacity();
        this.dfsUsed = from.getDfsUsed();
        this.remaining = from.getRemaining();
        this.numBlocks = from.getNumBlocks();
        this.alive = from.alive;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public long getLastUpdateMonotonic() {
        return lastUpdateMonotonic;
    }

    public void setLastUpdate(long wallClock, long monotonic) {
        this.lastUpdate = wallClock;
        this.lastUpdateMonotonic = monotonic;
    }

    /** Sum of the capacity of all healthy storages. */
    public long getCapacity() {
        return capacity;
    }

    /** Free space across all healthy storages. */
    public long getRemaining() {
        return remaining;
    }

    public long getDfsUsed() {
        return dfsUsed;
    }

    public void setDfsUsed(long dfsUsed) {
        this.dfsUsed = dfsUsed;
    }

    public int getNumBlocks() {
        return numBlocks;
    }

    public void setNumBlocks(int numBlocks) {
        this.numBlocks = numBlocks;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }
}
