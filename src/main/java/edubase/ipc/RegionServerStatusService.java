package edubase.ipc;

import edubase.ServerName;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Remote;
import java.util.List;

/**
 * Corresponds to RegionServerStatusService (RegionServerStatus.proto): RegionServer -&gt; master.
 * As in EduDFS (DatanodeProtocol) the master never calls on its own to ask whether a
 * RegionServer is alive - the RegionServer reports regularly.
 */
public interface RegionServerStatusService extends Remote {

    String NAME = "RegionServerStatusService";

    /** Load of a region in the report (corresponds to RegionLoad / RegionMetrics). */
    record RegionLoad(String encodedName, long memStoreSize, int storefiles, long storefileSize,
                      long readRequests, long writeRequests) implements Serializable {
    }

    /** At startup: "I am here". */
    void regionServerStartup(ServerName sn) throws IOException;

    /**
     * Heartbeat every hbase.regionserver.msginterval with the load of all online regions.
     * Throws YouAreDeadException if the master has already declared the server dead.
     */
    void regionServerReport(ServerName sn, List<RegionLoad> load) throws IOException;
}
