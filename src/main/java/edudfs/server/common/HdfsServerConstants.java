package edudfs.server.common;

/** Corresponds to org.apache.hadoop.hdfs.server.common.HdfsServerConstants. */
public final class HdfsServerConstants {

    private HdfsServerConstants() {
    }

    /**
     * When the NameNode starts a lease recovery, it takes over the lease itself
     * under this name. The original client thereby loses its write permission.
     */
    public static final String NAMENODE_LEASE_HOLDER = "HDFS_NameNode";

    // ------------------------------------------------- Cluster identity
    // Simplification: hard-wired. In the original, "hdfs namenode -format" generates these values
    // once, stores them in <dfs.namenode.name.dir>/current/VERSION, and every DataNode
    // fetches them via versionRequest() (NamespaceInfo) before registering. A DataNode
    // with a foreign clusterID is rejected ("Incompatible clusterIDs").

    /** Original: "CID-" + random UUID (NNStorage.newClusterID), e.g. CID-5d6c1f0e-.... */
    public static final String CLUSTER_ID = "CID-edudfs-0000-0000-0000-000000000001";

    /**
     * ID of the only block pool. Original (NNStorage.newBlockPoolID):
     * "BP-" + random number + "-" + IP of the NameNode + "-" + time of formatting in ms,
     * e.g. BP-526805057-192.168.1.10-1697712345678. With federation there are several
     * namespaces/NameNodes and thus several block pools on the same DataNode disks.
     */
    public static final String BLOCK_POOL_ID = "BP-1-127.0.0.1-1700000000000";

    /** Original: random number generated at format time (NNStorage.newNamespaceID). */
    public static final int NAMESPACE_ID = 1;

    /** DataNode layout version (Hadoop 3.x: -57). Stored in every VERSION file. */
    public static final int DATANODE_LAYOUT_VERSION = -57;

    /** State of a replica on a DataNode. */
    public enum ReplicaState {
        /** Completely written, immutable. */
        FINALIZED,
        /** Replica Being Written: a writer is currently writing. */
        RBW,
        /** Replica Waiting to be Recovered: RBW after a DataNode restart (writer gone). */
        RWR,
        /** Replica Under Recovery: currently being processed by a block recovery. */
        RUR
    }

    /** State of a block from the NameNode's point of view. */
    public enum BlockUCState {
        /** Block is complete and at least minReplication replicas have been reported. */
        COMPLETE,
        /** Block is currently being written. */
        UNDER_CONSTRUCTION,
        /** Lease has expired, block recovery is in progress. */
        UNDER_RECOVERY,
        /** Client has reported length+GS (commit), but too few DataNodes have reported FINALIZED yet. */
        COMMITTED
    }
}
