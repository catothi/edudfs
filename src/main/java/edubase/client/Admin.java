package edubase.client;

import edubase.HRegionLocation;
import edubase.ServerName;
import edubase.TableName;
import edubase.ipc.AdminService;
import edubase.regionserver.compactions.CompactionRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Admin (HBaseAdmin): administration. DDL goes to the
 * master; flush and compaction go - as in the original - directly to the RegionServers of the
 * table's regions.
 */
public class Admin implements Closeable {

    private final Connection connection;

    Admin(Connection connection) {
        this.connection = connection;
    }

    public void createTable(TableDescriptor desc) throws IOException {
        createTable(desc, null);
    }

    /** Pre-split: with n split keys the table starts with n+1 regions. */
    public void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException {
        connection.withMasterRetries(() -> {
            connection.getMaster().createTable(desc, splitKeys);
            return null;
        });
    }

    public void disableTable(TableName table) throws IOException {
        connection.withMasterRetries(() -> {
            connection.getMaster().disableTable(table);
            return null;
        });
        connection.clearRegionCache(table);
    }

    public void enableTable(TableName table) throws IOException {
        connection.withMasterRetries(() -> {
            connection.getMaster().enableTable(table);
            return null;
        });
        connection.clearRegionCache(table);
    }

    public void deleteTable(TableName table) throws IOException {
        connection.withMasterRetries(() -> {
            connection.getMaster().deleteTable(table);
            return null;
        });
        connection.clearRegionCache(table);
    }

    public boolean tableExists(TableName table) throws IOException {
        for (TableDescriptor d : listTableDescriptors()) {
            if (d.getTableName().equals(table)) {
                return true;
            }
        }
        return false;
    }

    public boolean isTableEnabled(TableName table) throws IOException {
        return connection.withMasterRetries(() -> connection.getMaster().isTableEnabled(table));
    }

    public List<TableDescriptor> listTableDescriptors() throws IOException {
        return connection.withMasterRetries(() -> connection.getMaster().listTableDescriptors());
    }

    public TableDescriptor getDescriptor(TableName table) throws IOException {
        return connection.withMasterRetries(() -> connection.getMaster().getDescriptor(table));
    }

    /** The regions of the table with their location (fresh from the master). */
    public List<HRegionLocation> getRegions(TableName table) throws IOException {
        return connection.locateRegions(table, true);
    }

    /** Flushes all regions of the table; returns the reports of the RegionServers. */
    public String flush(TableName table) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (HRegionLocation loc : getRegions(table)) {
            if (loc.getServerName() != null) {
                sb.append(connection.getAdmin(loc.getServerName()).flushRegion(loc.getRegion().getEncodedName()));
            }
        }
        return sb.toString();
    }

    public List<CompactionRecord> compact(TableName table) throws IOException {
        return compact(table, false);
    }

    public List<CompactionRecord> majorCompact(TableName table) throws IOException {
        return compact(table, true);
    }

    private List<CompactionRecord> compact(TableName table, boolean major) throws IOException {
        List<CompactionRecord> r = new ArrayList<>();
        for (HRegionLocation loc : getRegions(table)) {
            if (loc.getServerName() != null) {
                AdminService rs = connection.getAdmin(loc.getServerName());
                r.addAll(rs.compactRegion(loc.getRegion().getEncodedName(), major));
            }
        }
        return r;
    }

    /** EduBase: the last compactions of all RegionServers that serve regions of tables. */
    public List<CompactionRecord> getCompactionHistory() throws IOException {
        Set<ServerName> servers = new LinkedHashSet<>();
        for (TableDescriptor d : listTableDescriptors()) {
            if (isTableEnabled(d.getTableName())) {
                for (HRegionLocation loc : getRegions(d.getTableName())) {
                    if (loc.getServerName() != null) {
                        servers.add(loc.getServerName());
                    }
                }
            }
        }
        List<CompactionRecord> r = new ArrayList<>();
        for (ServerName sn : servers) {
            AdminService a = connection.getAdmin(sn);
            r.addAll(a.getCompactionHistory());
        }
        return r;
    }

    /** EduBase: master + all RegionServers (like ./hdfs dfs -metasave). */
    public String metaSave() throws IOException {
        return connection.withMasterRetries(() -> connection.getMaster().metaSave());
    }

    public String getClusterStatus(boolean detailed) throws IOException {
        return connection.withMasterRetries(() -> connection.getMaster().getClusterStatus(detailed));
    }

    public List<Result> scanMeta() throws IOException {
        return connection.withMasterRetries(() -> connection.getMaster().scanMeta());
    }

    @Override
    public void close() {
        // the connection holds the resources
    }
}
