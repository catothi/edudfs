package edudfs.server.namenode;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.FileUnderConstructionFeature.
 * Only files that are currently being written carry this "feature" -
 * it holds the name of the lease holder.
 */
public class FileUnderConstructionFeature {

    private String clientName;
    private final String clientMachine;

    public FileUnderConstructionFeature(String clientName, String clientMachine) {
        this.clientName = clientName;
        this.clientMachine = clientMachine;
    }

    public String getClientName() {
        return clientName;
    }

    /** Set to "HDFS_NameNode" during lease recovery (reassignLease). */
    void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getClientMachine() {
        return clientMachine;
    }
}
