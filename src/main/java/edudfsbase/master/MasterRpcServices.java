package edudfsbase.master;

import edudfsbase.HRegionLocation;
import edudfsbase.ServerName;
import edudfsbase.TableName;
import edudfsbase.client.Result;
import edudfsbase.client.TableDescriptor;
import edudfsbase.ipc.MasterService;
import edudfsbase.ipc.RegionServerStatusService;

import java.io.IOException;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.master.MasterRpcServices: the RPC endpoints of the master,
 * for clients (MasterService) and for RegionServers (RegionServerStatusService).
 */
public class MasterRpcServices implements MasterService, RegionServerStatusService {

    private final HMaster master;

    MasterRpcServices(HMaster master) {
        this.master = master;
    }

    // -------------------------------------------------------- MasterService
    @Override
    public void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException {
        master.createTable(desc, splitKeys);
    }

    @Override
    public void disableTable(TableName table) throws IOException {
        master.disableTable(table);
    }

    @Override
    public void enableTable(TableName table) throws IOException {
        master.enableTable(table);
    }

    @Override
    public void deleteTable(TableName table) throws IOException {
        master.deleteTable(table);
    }

    @Override
    public List<TableDescriptor> listTableDescriptors() throws IOException {
        return master.listTableDescriptors();
    }

    @Override
    public TableDescriptor getDescriptor(TableName table) throws IOException {
        return master.getDescriptor(table);
    }

    @Override
    public boolean isTableEnabled(TableName table) throws IOException {
        return master.isTableEnabled(table);
    }

    @Override
    public List<HRegionLocation> getTableRegions(TableName table) throws IOException {
        return master.getTableRegions(table);
    }

    @Override
    public List<Result> scanMeta() throws IOException {
        return master.scanMeta();
    }

    @Override
    public String metaSave() {
        return master.metaSave();
    }

    @Override
    public String getClusterStatus(boolean detailed) {
        return master.getClusterStatus(detailed);
    }

    // -------------------------------------------- RegionServerStatusService
    @Override
    public void regionServerStartup(ServerName sn) throws IOException {
        master.regionServerStartup(sn);
    }

    @Override
    public void regionServerReport(ServerName sn, List<RegionLoad> load) throws IOException {
        master.regionServerReport(sn, load);
    }
}
