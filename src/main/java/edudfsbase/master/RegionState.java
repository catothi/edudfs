package edudfsbase.master;

import edudfsbase.RegionInfo;
import edudfsbase.ServerName;

/**
 * Corresponds to org.apache.hadoop.hbase.master.RegionState (+ RegionStateNode): what the master
 * knows about a region - in the original this is the content of the region's row in hbase:meta
 * (info:regioninfo, info:server, info:state, info:seqnumDuringOpen).
 */
public class RegionState {

    /** States as in the original (subset). */
    public enum State {
        /** not open anywhere, waiting for assignment */
        OFFLINE,
        /** openRegion sent to a RegionServer */
        OPENING,
        OPEN,
        CLOSING,
        /** closed on purpose (table disabled) */
        CLOSED,
        /** its server crashed; may only be opened again after the WAL has been split */
        ABNORMALLY_CLOSED
    }

    private final RegionInfo region;
    private volatile State state;
    private volatile ServerName server;
    private volatile long openSeqNum;
    private volatile long stamp;

    RegionState(RegionInfo region, State state) {
        this.region = region;
        this.state = state;
        this.stamp = System.currentTimeMillis();
    }

    void set(State state, ServerName server) {
        this.state = state;
        this.server = server;
        this.stamp = System.currentTimeMillis();
    }

    void setOpenSeqNum(long openSeqNum) {
        this.openSeqNum = openSeqNum;
    }

    public RegionInfo getRegion() {
        return region;
    }

    public State getState() {
        return state;
    }

    public ServerName getServerName() {
        return server;
    }

    public long getOpenSeqNum() {
        return openSeqNum;
    }

    public long getStamp() {
        return stamp;
    }

    @Override
    public String toString() {
        return region.getEncodedName() + " state=" + state + (server == null ? "" : ", server=" + server);
    }
}
