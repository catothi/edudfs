package edubase.ipc;

import edubase.HRegionLocation;
import edubase.TableName;
import edubase.client.Result;
import edubase.client.TableDescriptor;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;

/**
 * Corresponds to MasterService (Master.proto): client (Admin) -&gt; master. DDL and cluster information.
 *
 * {@link #getTableRegions} replaces the lookup in hbase:meta: in the original, the client finds the
 * location of hbase:meta in ZooKeeper, reads the region's row from hbase:meta on that RegionServer
 * and caches it. In EduBase the master keeps the content of hbase:meta in memory and answers directly.
 */
public interface MasterService extends Remote {

    String NAME = "MasterService";

    void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException;

    void disableTable(TableName table) throws IOException;

    void enableTable(TableName table) throws IOException;

    void deleteTable(TableName table) throws IOException;

    List<TableDescriptor> listTableDescriptors() throws IOException;

    TableDescriptor getDescriptor(TableName table) throws IOException;

    boolean isTableEnabled(TableName table) throws IOException;

    /** "Meta lookup": all regions of the table with their current RegionServer (sorted by start key). */
    List<HRegionLocation> getTableRegions(TableName table) throws IOException;

    /** The content of hbase:meta in its original row/column form (for scan 'hbase:meta'). */
    List<Result> scanMeta() throws IOException;

    /** EduBase: master view (servers, regions, states, procedures) + metaSave of every RegionServer. */
    String metaSave() throws IOException;

    /** Short cluster status (shell: status). */
    String getClusterStatus(boolean detailed) throws IOException;
}
