package edubase.master;

import edubase.HBaseConfiguration;
import edubase.ServerName;
import edubase.YouAreDeadException;
import edubase.ipc.RegionServerStatusService.RegionLoad;
import edudfs.common.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hbase.master.ServerManager: which RegionServers are alive.
 *
 * In the original a RegionServer counts as dead when its ephemeral node in ZooKeeper disappears
 * (session timeout). EduBase has no ZooKeeper: every report (heartbeat) refreshes a time stamp;
 * whoever stays silent for longer than zookeeper.session.timeout is "expired".
 */
public class ServerManager {

    private static final String LOG = "ServerManager";

    private final Map<ServerName, Long> lastReport = new HashMap<>();
    private final Map<ServerName, List<RegionLoad>> loads = new HashMap<>();
    private final Set<ServerName> deadServers = new LinkedHashSet<>();

    synchronized void regionServerStartup(ServerName sn) throws YouAreDeadException {
        checkIsDead(sn);
        if (!lastReport.containsKey(sn)) {
            Log.info(LOG, "Registering regionserver=" + sn);
        }
        lastReport.put(sn, System.currentTimeMillis());
    }

    /** Heartbeat. An unknown (e.g. after a master restart) but not dead server is registered. */
    synchronized boolean regionServerReport(ServerName sn, List<RegionLoad> load) throws YouAreDeadException {
        checkIsDead(sn);
        boolean isNew = !lastReport.containsKey(sn);
        if (isNew) {
            Log.info(LOG, "Registering regionserver=" + sn + " from its report");
        }
        lastReport.put(sn, System.currentTimeMillis());
        loads.put(sn, load);
        return isNew;
    }

    private void checkIsDead(ServerName sn) throws YouAreDeadException {
        if (deadServers.contains(sn)) {
            throw new YouAreDeadException("Server REPORT rejected; currently processing " + sn + " as dead server");
        }
    }

    /** Servers whose last report is older than the session timeout. */
    synchronized List<ServerName> findExpiredServers() {
        long now = System.currentTimeMillis();
        List<ServerName> r = new ArrayList<>();
        for (Map.Entry<ServerName, Long> e : lastReport.entrySet()) {
            if (now - e.getValue() > HBaseConfiguration.SESSION_TIMEOUT) {
                r.add(e.getKey());
            }
        }
        return r;
    }

    synchronized void expireServer(ServerName sn) {
        lastReport.remove(sn);
        loads.remove(sn);
        deadServers.add(sn);
        Log.warn(LOG, "Expired regionserver " + sn + " (no report for more than zookeeper.session.timeout="
                + HBaseConfiguration.SESSION_TIMEOUT + " ms) -> ServerCrashProcedure");
    }

    synchronized boolean isServerOnline(ServerName sn) {
        return lastReport.containsKey(sn);
    }

    synchronized boolean isServerDead(ServerName sn) {
        return deadServers.contains(sn);
    }

    synchronized void markDead(ServerName sn) {
        deadServers.add(sn);
    }

    synchronized List<ServerName> getOnlineServersList() {
        List<ServerName> r = new ArrayList<>(lastReport.keySet());
        Collections.sort(r);
        return r;
    }

    synchronized Set<ServerName> getDeadServers() {
        return new HashSet<>(deadServers);
    }

    synchronized List<RegionLoad> getLoad(ServerName sn) {
        return loads.getOrDefault(sn, List.of());
    }

    synchronized long getLastReport(ServerName sn) {
        return lastReport.getOrDefault(sn, 0L);
    }
}
