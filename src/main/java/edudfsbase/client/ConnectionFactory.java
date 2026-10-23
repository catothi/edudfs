package edudfsbase.client;

/**
 * Corresponds to org.apache.hadoop.hbase.client.ConnectionFactory. The address of the master comes from
 * hbase-site.xml (original: the client finds everything via the ZooKeeper quorum in hbase-site.xml).
 */
public final class ConnectionFactory {

    private ConnectionFactory() {
    }

    public static Connection createConnection() {
        return new Connection();
    }
}
