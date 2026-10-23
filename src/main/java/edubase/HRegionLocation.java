package edubase;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hbase.HRegionLocation: which RegionServer serves which region.
 * This is exactly the information of a row in hbase:meta (info:regioninfo + info:server).
 */
public final class HRegionLocation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RegionInfo region;
    private final ServerName serverName;
    private final long seqNum;

    public HRegionLocation(RegionInfo region, ServerName serverName, long seqNum) {
        this.region = region;
        this.serverName = serverName;
        this.seqNum = seqNum;
    }

    public RegionInfo getRegion() {
        return region;
    }

    /** null if the region is currently not open anywhere (e.g. during recovery). */
    public ServerName getServerName() {
        return serverName;
    }

    /** Sequence ID with which the region was opened (info:seqnumDuringOpen). */
    public long getSeqNum() {
        return seqNum;
    }

    @Override
    public String toString() {
        return "region=" + region.getRegionNameAsString() + ", hostname=" + serverName + ", seqNum=" + seqNum;
    }
}
