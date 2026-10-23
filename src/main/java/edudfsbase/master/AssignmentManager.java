package edudfsbase.master;

import edudfsbase.RegionInfo;
import edudfsbase.ServerName;
import edudfsbase.TableName;
import edudfsbase.client.TableDescriptor;
import edudfsbase.util.Bytes;
import edudfs.common.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Corresponds to org.apache.hadoop.hbase.master.assignment.AssignmentManager (+ RegionStates,
 * TableStateManager): which region is open where, and which tables are enabled.
 *
 * Assign = choose a RegionServer (here: the one with the fewest regions; original: the
 * LoadBalancer) and send it openRegion. The RegionServer opens the region from HDFS - the data is not
 * copied anywhere, because every RegionServer can read every HFile.
 *
 * Simplification: the original runs every (un)assignment as a persistent procedure
 * (TransitRegionStateProcedure, stored in the master's procedure WAL) so that it survives a master
 * crash; here they are simple method calls.
 */
public class AssignmentManager {

    private static final String LOG = "AssignmentManager";

    private final HMaster master;
    private final Map<String, RegionState> regionStates = new ConcurrentHashMap<>();
    private final Map<TableName, TableDescriptor> tables = new ConcurrentHashMap<>();
    /** Corresponds to TableState: ENABLED (true) or DISABLED (false). */
    private final Map<TableName, Boolean> tableEnabled = new ConcurrentHashMap<>();

    AssignmentManager(HMaster master) {
        this.master = master;
    }

    // ------------------------------------------------------------ tables
    void addTable(TableDescriptor htd, Collection<RegionInfo> regions, boolean enabled) {
        tables.put(htd.getTableName(), htd);
        tableEnabled.put(htd.getTableName(), enabled);
        for (RegionInfo ri : regions) {
            regionStates.put(ri.getEncodedName(),
                    new RegionState(ri, enabled ? RegionState.State.OFFLINE : RegionState.State.CLOSED));
        }
    }

    void removeTable(TableName table) {
        tables.remove(table);
        tableEnabled.remove(table);
        regionStates.values().removeIf(rs -> rs.getRegion().getTable().equals(table));
    }

    TableDescriptor getTableDescriptor(TableName table) {
        return tables.get(table);
    }

    Collection<TableDescriptor> getTableDescriptors() {
        return tables.values();
    }

    boolean isTableEnabled(TableName table) {
        return tableEnabled.getOrDefault(table, false);
    }

    void setTableEnabled(TableName table, boolean enabled) {
        tableEnabled.put(table, enabled);
    }

    // ------------------------------------------------------------ regions
    /** The regions of a table, sorted by start key. */
    List<RegionState> getTableRegionStates(TableName table) {
        List<RegionState> r = new ArrayList<>();
        for (RegionState rs : regionStates.values()) {
            if (rs.getRegion().getTable().equals(table)) {
                r.add(rs);
            }
        }
        r.sort(Comparator.comparing(rs -> rs.getRegion().getStartKey(), Bytes::compareTo));
        return r;
    }

    List<RegionState> getAllRegionStates() {
        List<RegionState> r = new ArrayList<>(regionStates.values());
        r.sort(Comparator.comparing((RegionState rs) -> rs.getRegion().getTable())
                .thenComparing(rs -> rs.getRegion().getStartKey(), Bytes::compareTo));
        return r;
    }

    /** Regions that a server serves (or is opening) - what a crash of this server affects. */
    List<RegionState> getRegionsOnServer(ServerName sn) {
        List<RegionState> r = new ArrayList<>();
        for (RegionState rs : regionStates.values()) {
            if (sn.equals(rs.getServerName()) && (rs.getState() == RegionState.State.OPEN
                    || rs.getState() == RegionState.State.OPENING)) {
                r.add(rs);
            }
        }
        return r;
    }

    /** After a master restart: a RegionServer reports a region as open. */
    void regionReportedOnline(String encodedName, ServerName sn) {
        RegionState rs = regionStates.get(encodedName);
        if (rs != null && rs.getState() != RegionState.State.OPEN) {
            rs.set(RegionState.State.OPEN, sn);
            Log.info(LOG, "Region " + rs.getRegion().getShortName() + " reported open on " + sn);
        }
    }

    // --------------------------------------------------------- (un)assign
    /** Opens the region on the RegionServer with the fewest regions. false if that did not work. */
    boolean assign(RegionState rs) {
        ServerName target;
        synchronized (rs) {
            RegionState.State s = rs.getState();
            if (s != RegionState.State.OFFLINE && s != RegionState.State.CLOSED) {
                return s == RegionState.State.OPEN;
            }
            target = chooseServer();
            if (target == null) {
                return false;
            }
            rs.set(RegionState.State.OPENING, target);
        }
        TableDescriptor htd = tables.get(rs.getRegion().getTable());
        try {
            long seq = master.getAdminService(target).openRegion(rs.getRegion(), htd);
            rs.setOpenSeqNum(seq);
            rs.set(RegionState.State.OPEN, target);
            Log.info(LOG, "Assigned " + rs.getRegion().getRegionNameAsString() + " to " + target
                    + ", openSeqNum=" + seq);
            return true;
        } catch (IOException e) {
            Log.warn(LOG, "Failed to open " + rs.getRegion().getShortName() + " on " + target + ": " + e.getMessage());
            master.dropAdminService(target);
            rs.set(RegionState.State.OFFLINE, null);
            return false;
        }
    }

    private ServerName chooseServer() {
        List<ServerName> servers = master.getServerManager().getOnlineServersList();
        if (servers.isEmpty()) {
            return null;
        }
        Map<ServerName, Integer> count = new HashMap<>();
        for (ServerName sn : servers) {
            count.put(sn, 0);
        }
        for (RegionState rs : regionStates.values()) {
            ServerName sn = rs.getServerName();
            if (sn != null && count.containsKey(sn)) {
                count.merge(sn, 1, Integer::sum);
            }
        }
        ServerName best = null;
        for (ServerName sn : servers) {
            if (best == null || count.get(sn) < count.get(best)) {
                best = sn;
            }
        }
        return best;
    }

    void unassign(RegionState rs) throws IOException {
        ServerName sn;
        synchronized (rs) {
            if (rs.getState() != RegionState.State.OPEN) {
                rs.set(RegionState.State.CLOSED, null);
                return;
            }
            sn = rs.getServerName();
            rs.set(RegionState.State.CLOSING, sn);
        }
        master.getAdminService(sn).closeRegion(rs.getRegion().getEncodedName());
        rs.set(RegionState.State.CLOSED, null);
        Log.info(LOG, "Closed " + rs.getRegion().getRegionNameAsString() + " on " + sn);
    }

    /** Chore: open all OFFLINE regions of enabled tables (e.g. after a failed assign). */
    void assignOfflineRegions() {
        for (RegionState rs : getAllRegionStates()) {
            if (rs.getState() == RegionState.State.OFFLINE && isTableEnabled(rs.getRegion().getTable())) {
                assign(rs);
            }
        }
    }

    void markAbnormallyClosed(RegionState rs) {
        rs.set(RegionState.State.ABNORMALLY_CLOSED, rs.getServerName());
    }

    void markOffline(RegionState rs) {
        rs.set(RegionState.State.OFFLINE, null);
    }
}
