package edudfsbase.master;

import edudfsbase.Cell;
import edudfsbase.HBaseConfiguration;
import edudfsbase.HConstants;
import edudfsbase.HRegionLocation;
import edudfsbase.KeyValue;
import edudfsbase.PleaseHoldException;
import edudfsbase.RegionInfo;
import edudfsbase.ServerName;
import edudfsbase.TableExistsException;
import edudfsbase.TableName;
import edudfsbase.TableNotDisabledException;
import edudfsbase.TableNotEnabledException;
import edudfsbase.TableNotFoundException;
import edudfsbase.client.ColumnFamilyDescriptor;
import edudfsbase.client.Result;
import edudfsbase.client.TableDescriptor;
import edudfsbase.fs.CommonFSUtils;
import edudfsbase.fs.HFileSystem;
import edudfsbase.ipc.AdminService;
import edudfsbase.ipc.MasterService;
import edudfsbase.ipc.RegionServerStatusService;
import edudfsbase.ipc.RegionServerStatusService.RegionLoad;
import edudfsbase.regionserver.HRegion;
import edudfsbase.util.Bytes;
import edudfsbase.util.EnvironmentEdgeManager;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Corresponds to org.apache.hadoop.hbase.master.HMaster: the coordinator of the cluster.
 *
 * The master is NOT in the data path: clients read and write directly on the RegionServers. It is
 * responsible for DDL (create/disable/enable/drop table), region assignment, recognizing dead
 * RegionServers and recovering their regions (ServerCrashProcedure).
 *
 * Simplifications compared to the original:
 * <ul>
 *   <li>no ZooKeeper: RegionServers report directly to the master (see ServerManager),</li>
 *   <li>hbase:meta is not a real table on a RegionServer, but lives in the master's memory
 *       (AssignmentManager); after a master restart it is rebuilt from HDFS,</li>
 *   <li>no backup master, no load balancer (only "fewest regions"), no region splits/merges,</li>
 *   <li>no procedure store (procedures do not survive a master crash).</li>
 * </ul>
 */
public class HMaster {

    private static final String LOG = "HMaster";

    private final int port;
    private ServerName serverName;
    private HFileSystem fs;
    private MasterFileSystem mfs;
    private final ServerManager serverManager = new ServerManager();
    private final AssignmentManager assignmentManager = new AssignmentManager(this);
    private MasterRpcServices rpcServices;
    private Registry registry;
    private final Map<ServerName, AdminService> rsStubs = new ConcurrentHashMap<>();
    private final List<ServerCrashProcedure> procedures = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean initialized;
    private volatile boolean stopped;
    private Thread choreThread;

    public HMaster(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        System.setProperty("java.rmi.server.hostname", HBaseConfiguration.HOSTNAME);
        serverName = new ServerName(HBaseConfiguration.HOSTNAME, port, EnvironmentEdgeManager.currentTime());
        fs = new HFileSystem();
        mfs = new MasterFileSystem(fs);
        rpcServices = new MasterRpcServices(this);
        Remote stub = UnicastRemoteObject.exportObject(rpcServices, 0);
        registry = LocateRegistry.createRegistry(port);
        registry.rebind(MasterService.NAME, stub);
        registry.rebind(RegionServerStatusService.NAME, stub);
        Log.info(LOG, "HMaster " + serverName + " started, hbase.rootdir=" + CommonFSUtils.getRootDir());
        choreThread = new Thread(this::run, "HMaster " + serverName);
        choreThread.setDaemon(true);
        choreThread.start();
    }

    /** finishActiveMasterInitialization, then the chores (dead servers, unassigned regions). */
    private void run() {
        try {
            finishActiveMasterInitialization();
        } catch (IOException | InterruptedException e) {
            if (!stopped) {
                Log.warn(LOG, "Master initialization failed: " + e);
            }
            return;
        }
        while (!stopped) {
            try {
                for (ServerName sn : serverManager.findExpiredServers()) {
                    expireServer(sn);
                }
                assignmentManager.assignOfflineRegions();
                Thread.sleep(500);
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                Log.warn(LOG, "Chore failed: " + e);
            }
        }
    }

    /**
     * Corresponds to HMaster.finishActiveMasterInitialization:
     * (1) wait for RegionServers, (2) load tables and regions, (3) split WALs of servers that are
     * not alive any more (they crashed while there was no master), (4) assign the remaining regions.
     */
    private void finishActiveMasterInitialization() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        while (!stopped && (serverManager.getOnlineServersList().size() < HBaseConfiguration.WAIT_ON_RS_MIN
                || System.currentTimeMillis() - start < HBaseConfiguration.WAIT_ON_RS_TIMEOUT)) {
            Thread.sleep(100);
        }
        List<ServerName> online = serverManager.getOnlineServersList();
        Log.info(LOG, "Waited " + (System.currentTimeMillis() - start) + " ms for RegionServers, "
                + online.size() + " online: " + online);
        for (Map.Entry<TableDescriptor, List<RegionInfo>> t : mfs.loadTables().entrySet()) {
            assignmentManager.addTable(t.getKey(), t.getValue(), true);
        }
        for (ServerName sn : online) {
            for (RegionLoad l : serverManager.getLoad(sn)) {
                assignmentManager.regionReportedOnline(l.encodedName(), sn);
            }
        }
        for (HdfsFileStatus st : fs.listStatus(CommonFSUtils.getWALRootDir())) {
            String name = CommonFSUtils.getName(st.getPath());
            ServerName sn = ServerName.valueOf(name.replace(HConstants.SPLITTING_EXT, ""));
            if (!serverManager.isServerOnline(sn)) {
                Log.info(LOG, "Found WALs of dead server " + sn + " (not online) -> split them before assigning");
                serverManager.markDead(sn);
                ServerCrashProcedure.splitLogs(fs, sn, s -> Log.info(LOG, s));
            }
        }
        initialized = true;
        Log.info(LOG, "Master has completed initialization, " + assignmentManager.getTableDescriptors().size()
                + " table(s)");
        assignmentManager.assignOfflineRegions();
    }

    private void expireServer(ServerName sn) {
        serverManager.expireServer(sn);
        dropAdminService(sn);
        ServerCrashProcedure scp = new ServerCrashProcedure(this, sn);
        procedures.add(scp);
        Thread t = new Thread(scp, "ServerCrashProcedure " + sn);
        t.setDaemon(true);
        t.start();
    }

    // ------------------------------------------------------- RegionServers
    void regionServerStartup(ServerName sn) throws IOException {
        serverManager.regionServerStartup(sn);
    }

    void regionServerReport(ServerName sn, List<RegionLoad> load) throws IOException {
        boolean isNew = serverManager.regionServerReport(sn, load);
        if (isNew && initialized) {
            for (RegionLoad l : load) {
                assignmentManager.regionReportedOnline(l.encodedName(), sn);
            }
        }
    }

    AdminService getAdminService(ServerName sn) throws IOException {
        AdminService s = rsStubs.get(sn);
        if (s == null) {
            try {
                s = (AdminService) LocateRegistry.getRegistry(sn.getHostname(), sn.getPort()).lookup(AdminService.NAME);
            } catch (NotBoundException e) {
                throw new IOException(e);
            }
            rsStubs.put(sn, s);
        }
        return s;
    }

    void dropAdminService(ServerName sn) {
        rsStubs.remove(sn);
    }

    // ----------------------------------------------------------------- DDL
    private void checkInitialized() throws PleaseHoldException {
        if (!initialized) {
            throw new PleaseHoldException("Master is initializing");
        }
    }

    /**
     * Corresponds to CreateTableProcedure: write .tableinfo, create the region directories with
     * .regioninfo, assign the regions. Split keys pre-split the table into several regions.
     */
    synchronized void createTable(TableDescriptor htd, byte[][] splitKeys) throws IOException {
        checkInitialized();
        TableName name = htd.getTableName();
        if (assignmentManager.getTableDescriptor(name) != null) {
            throw new TableExistsException(name.toString());
        }
        long regionId = EnvironmentEdgeManager.currentTime();
        List<RegionInfo> regions = new ArrayList<>();
        byte[] start = HConstants.EMPTY_START_ROW;
        byte[][] keys = splitKeys == null ? new byte[0][] : splitKeys.clone();
        Arrays.sort(keys, Bytes::compareTo);
        for (byte[] k : keys) {
            regions.add(new RegionInfo(name, start, k, regionId));
            start = k;
        }
        regions.add(new RegionInfo(name, start, HConstants.EMPTY_END_ROW, regionId));
        mfs.writeTableDescriptor(htd);
        for (RegionInfo ri : regions) {
            HRegion.createRegionDir(fs, ri);
        }
        assignmentManager.addTable(htd, regions, true);
        Log.info(LOG, "Created table " + name + " with " + regions.size() + " region(s)");
        for (RegionState rs : assignmentManager.getTableRegionStates(name)) {
            assignmentManager.assign(rs);
        }
    }

    synchronized void disableTable(TableName table) throws IOException {
        checkInitialized();
        checkTableExists(table);
        if (!assignmentManager.isTableEnabled(table)) {
            throw new TableNotEnabledException(table.toString());
        }
        assignmentManager.setTableEnabled(table, false);
        for (RegionState rs : assignmentManager.getTableRegionStates(table)) {
            assignmentManager.unassign(rs);
        }
        Log.info(LOG, "Disabled table " + table);
    }

    synchronized void enableTable(TableName table) throws IOException {
        checkInitialized();
        checkTableExists(table);
        if (assignmentManager.isTableEnabled(table)) {
            throw new TableNotDisabledException(table.toString());
        }
        assignmentManager.setTableEnabled(table, true);
        for (RegionState rs : assignmentManager.getTableRegionStates(table)) {
            assignmentManager.assign(rs);
        }
        Log.info(LOG, "Enabled table " + table);
    }

    synchronized void deleteTable(TableName table) throws IOException {
        checkInitialized();
        TableDescriptor htd = checkTableExists(table);
        if (assignmentManager.isTableEnabled(table)) {
            throw new TableNotDisabledException(table.toString());
        }
        mfs.deleteTable(htd);
        assignmentManager.removeTable(table);
        Log.info(LOG, "Deleted table " + table);
    }

    private TableDescriptor checkTableExists(TableName table) throws TableNotFoundException {
        TableDescriptor htd = assignmentManager.getTableDescriptor(table);
        if (htd == null) {
            throw new TableNotFoundException(table.toString());
        }
        return htd;
    }

    List<TableDescriptor> listTableDescriptors() throws IOException {
        checkInitialized();
        List<TableDescriptor> r = new ArrayList<>(assignmentManager.getTableDescriptors());
        r.sort((a, b) -> a.getTableName().compareTo(b.getTableName()));
        return r;
    }

    TableDescriptor getDescriptor(TableName table) throws IOException {
        checkInitialized();
        return checkTableExists(table);
    }

    boolean isTableEnabled(TableName table) throws IOException {
        checkInitialized();
        checkTableExists(table);
        return assignmentManager.isTableEnabled(table);
    }

    /** The "meta lookup" of the client. */
    List<HRegionLocation> getTableRegions(TableName table) throws IOException {
        checkInitialized();
        checkTableExists(table);
        if (!assignmentManager.isTableEnabled(table)) {
            throw new TableNotEnabledException(table + " is disabled.");
        }
        List<HRegionLocation> r = new ArrayList<>();
        for (RegionState rs : assignmentManager.getTableRegionStates(table)) {
            r.add(new HRegionLocation(rs.getRegion(),
                    rs.getState() == RegionState.State.OPEN ? rs.getServerName() : null, rs.getOpenSeqNum()));
        }
        return r;
    }

    /**
     * The content of hbase:meta as the original stores it: one row per region (row key = region name)
     * with the columns info:regioninfo, info:seqnumDuringOpen, info:server, info:serverstartcode,
     * info:state - plus one row per table with table:state.
     */
    List<Result> scanMeta() throws IOException {
        checkInitialized();
        List<Result> r = new ArrayList<>();
        byte[] info = Bytes.toBytes("info");
        for (TableDescriptor htd : listTableDescriptors()) {
            byte[] row = Bytes.toBytes(htd.getTableName().toString());
            r.add(new Result(List.of(new KeyValue(row, Bytes.toBytes("table"), Bytes.toBytes("state"), 0,
                    Bytes.toBytes(assignmentManager.isTableEnabled(htd.getTableName()) ? "ENABLED" : "DISABLED"))),
                    null, null));
            for (RegionState rs : assignmentManager.getTableRegionStates(htd.getTableName())) {
                byte[] rrow = Bytes.toBytes(rs.getRegion().getRegionNameAsString());
                long ts = rs.getStamp();
                List<Cell> cells = new ArrayList<>();
                cells.add(new KeyValue(rrow, info, Bytes.toBytes("regioninfo"), ts,
                        Bytes.toBytes(rs.getRegion().toString())));
                if (rs.getServerName() != null) {
                    cells.add(new KeyValue(rrow, info, Bytes.toBytes("seqnumDuringOpen"), ts,
                            Bytes.toBytes(String.valueOf(rs.getOpenSeqNum()))));
                    cells.add(new KeyValue(rrow, info, Bytes.toBytes("server"), ts,
                            Bytes.toBytes(rs.getServerName().getAddress())));
                    cells.add(new KeyValue(rrow, info, Bytes.toBytes("serverstartcode"), ts,
                            Bytes.toBytes(String.valueOf(rs.getServerName().getStartcode()))));
                }
                cells.add(new KeyValue(rrow, info, Bytes.toBytes("state"), ts, Bytes.toBytes(rs.getState().name())));
                r.add(new Result(cells, null, null));
            }
        }
        return r;
    }

    // ------------------------------------------------------------ metasave
    String metaSave() {
        StringBuilder sb = new StringBuilder();
        sb.append("HMaster ").append(serverName).append(initialized ? "" : "  (INITIALIZING)").append('\n');
        List<ServerName> online = serverManager.getOnlineServersList();
        sb.append("Online RegionServers (").append(online.size()).append("):\n");
        long now = System.currentTimeMillis();
        for (ServerName sn : online) {
            long mem = 0;
            int files = 0;
            for (RegionLoad l : serverManager.getLoad(sn)) {
                mem += l.memStoreSize();
                files += l.storefiles();
            }
            sb.append(String.format(Locale.ROOT, "  %-36s last report %.1f s ago, %d region(s), MemStore %d bytes,"
                            + " %d HFile(s)%n", sn, (now - serverManager.getLastReport(sn)) / 1000.0,
                    serverManager.getLoad(sn).size(), mem, files));
        }
        if (!serverManager.getDeadServers().isEmpty()) {
            sb.append("Dead RegionServers: ").append(serverManager.getDeadServers()).append('\n');
        }
        sb.append("hbase:meta (in master memory):\n");
        for (TableDescriptor htd : assignmentManager.getTableDescriptors()) {
            TableName t = htd.getTableName();
            sb.append("  table ").append(t).append("  ")
                    .append(assignmentManager.isTableEnabled(t) ? "ENABLED" : "DISABLED");
            for (ColumnFamilyDescriptor f : htd.getColumnFamilies()) {
                sb.append("  ").append(f);
            }
            sb.append('\n');
            for (RegionState rs : assignmentManager.getTableRegionStates(t)) {
                sb.append(String.format("    %s  ['%s', '%s')  %-17s %s%n", rs.getRegion().getEncodedName(),
                        Bytes.toStringBinary(rs.getRegion().getStartKey()),
                        Bytes.toStringBinary(rs.getRegion().getEndKey()),
                        rs.getState(), rs.getServerName() == null ? "" : "on " + rs.getServerName()));
            }
        }
        synchronized (procedures) {
            if (!procedures.isEmpty()) {
                sb.append("Procedures:\n");
                for (ServerCrashProcedure p : procedures) {
                    sb.append("  pid=").append(p.getPid()).append(" ServerCrashProcedure server=")
                            .append(p.getServerName()).append("  ").append(p.getState()).append('\n');
                    for (String s : p.getSteps()) {
                        sb.append("      ").append(s).append('\n');
                    }
                }
            }
        }
        for (ServerName sn : online) {
            sb.append('\n');
            try {
                sb.append(getAdminService(sn).metaSave());
            } catch (IOException e) {
                sb.append("RegionServer ").append(sn).append(": not reachable (").append(e.getMessage()).append(")\n");
            }
        }
        return sb.toString();
    }

    String getClusterStatus(boolean detailed) {
        List<ServerName> online = serverManager.getOnlineServersList();
        int regions = 0;
        for (ServerName sn : online) {
            regions += serverManager.getLoad(sn).size();
        }
        StringBuilder sb = new StringBuilder(String.format(Locale.ROOT,
                "1 active master, 0 backup masters, %d servers, %d dead, %.4f average load%n", online.size(),
                serverManager.getDeadServers().size(), online.isEmpty() ? 0.0 : (double) regions / online.size()));
        if (detailed) {
            for (ServerName sn : online) {
                sb.append("    ").append(sn).append('\n');
                for (RegionLoad l : serverManager.getLoad(sn)) {
                    sb.append(String.format("        %s: memstoreSize=%d, storefiles=%d, storefileSize=%d, "
                                    + "readRequestsCount=%d, writeRequestsCount=%d%n", l.encodedName(),
                            l.memStoreSize(),
                            l.storefiles(), l.storefileSize(), l.readRequests(), l.writeRequests()));
                }
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------- getters
    ServerManager getServerManager() {
        return serverManager;
    }

    AssignmentManager getAssignmentManager() {
        return assignmentManager;
    }

    HFileSystem getFileSystem() {
        return fs;
    }

    public ServerName getServerName() {
        return serverName;
    }

    public boolean isInitialized() {
        return initialized;
    }

    /** Number of regions in state OPEN (for tests and the minicluster). */
    public int getNumberOfOpenRegions() {
        int n = 0;
        for (RegionState rs : assignmentManager.getAllRegionStates()) {
            if (rs.getState() == RegionState.State.OPEN) {
                n++;
            }
        }
        return n;
    }

    public int getNumberOfRegions() {
        return assignmentManager.getAllRegionStates().size();
    }

    public List<ServerCrashProcedure> getProcedures() {
        synchronized (procedures) {
            return new ArrayList<>(procedures);
        }
    }

    public void stop() {
        stopped = true;
        if (choreThread != null) {
            choreThread.interrupt();
        }
        try {
            UnicastRemoteObject.unexportObject(rpcServices, true);
            UnicastRemoteObject.unexportObject(registry, true);
        } catch (IOException ignored) {
            // already stopped
        }
        try {
            fs.close();
        } catch (IOException ignored) {
            // nothing open
        }
        Log.info(LOG, "HMaster " + serverName + " stopped");
    }

    public static void main(String[] args) throws Exception {
        HMaster m = new HMaster(HBaseConfiguration.MASTER_PORT);
        m.start();
        Runtime.getRuntime().addShutdownHook(new Thread(m::stop));
        Thread.currentThread().join();
    }
}
