package edudfsbase.regionserver;

import edudfsbase.Cell;
import edudfsbase.HBaseConfiguration;
import edudfsbase.NotServingRegionException;
import edudfsbase.RegionInfo;
import edudfsbase.ServerName;
import edudfsbase.YouAreDeadException;
import edudfsbase.client.TableDescriptor;
import edudfsbase.codec.KeyValueCodec;
import edudfsbase.fs.HFileSystem;
import edudfsbase.io.hfile.LruBlockCache;
import edudfsbase.ipc.AdminService;
import edudfsbase.ipc.ClientService;
import edudfsbase.ipc.RegionServerStatusService;
import edudfsbase.regionserver.compactions.CompactionRecord;
import edudfsbase.util.Bytes;
import edudfsbase.util.EnvironmentEdgeManager;
import edudfsbase.wal.FSHLog;
import edudfs.common.Log;
import edudfs.protocol.BlockLocalPathInfo;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.HRegionServer: serves regions.
 *
 * <ul>
 *   <li>ONE WAL ({@link FSHLog}) for all its regions, ONE BlockCache.</li>
 *   <li>Threads: MemStoreFlusher, CompactSplit, LogRoller and the report thread, which sends a
 *       heartbeat with the region loads to the master every hbase.regionserver.msginterval.</li>
 *   <li>RPC ({@link RSRpcServices}): ClientService (get/mutate/scan) and AdminService
 *       (open/close/flush/compact region).</li>
 * </ul>
 * The RegionServer holds no data of its own on local disk - HFiles and WAL are in HDFS. That is why
 * another RegionServer can take over its regions after a crash.
 */
public class HRegionServer {

    private static final String LOG = "HRegionServer";
    private static final int COMPACTION_HISTORY_SIZE = 20;

    private final int port;
    private ServerName serverName;
    private HFileSystem fs;
    private FSHLog wal;
    private LruBlockCache blockCache;
    private final ConcurrentHashMap<String, HRegion> onlineRegions = new ConcurrentHashMap<>();
    private RSRpcServices rpcServices;
    private Registry registry;
    private MemStoreFlusher cacheFlusher;
    private CompactSplit compactSplitThread;
    private Thread walRoller;
    private Thread reportThread;
    private final Deque<CompactionRecord> compactionHistory = new ArrayDeque<>();
    private volatile boolean stopped;
    private volatile boolean aborted;

    public HRegionServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        System.setProperty("java.rmi.server.hostname", HBaseConfiguration.HOSTNAME);
        serverName = new ServerName(HBaseConfiguration.HOSTNAME, port, EnvironmentEdgeManager.currentTime());
        fs = new HFileSystem();
        blockCache = new LruBlockCache((long) (Runtime.getRuntime().maxMemory() * HBaseConfiguration.BLOCK_CACHE_SIZE));
        wal = new FSHLog(fs, serverName);
        rpcServices = new RSRpcServices(this);
        Remote stub = UnicastRemoteObject.exportObject(rpcServices, 0);
        registry = LocateRegistry.createRegistry(port);
        registry.rebind(ClientService.NAME, stub);
        registry.rebind(AdminService.NAME, stub);

        cacheFlusher = new MemStoreFlusher(serverName.toString());
        cacheFlusher.start();
        compactSplitThread = new CompactSplit(this);
        walRoller = new Thread(this::logRollerLoop, "LogRoller " + serverName);
        walRoller.setDaemon(true);
        walRoller.start();
        reportThread = new Thread(this::reportLoop, "RS report " + serverName);
        reportThread.setDaemon(true);
        reportThread.start();
        Log.info(LOG, "RegionServer " + serverName + " started, RPC on port " + port);
    }

    // ------------------------------------------------------------ chores
    /** Corresponds to LogRoller: rolls the WAL when it is large or old enough. */
    private void logRollerLoop() {
        while (!stopped) {
            try {
                Thread.sleep(HBaseConfiguration.THREAD_WAKE_FREQUENCY);
                if (wal.isRollRequested()) {
                    wal.rollWriter();
                }
            } catch (InterruptedException e) {
                return;
            } catch (IOException e) {
                if (!stopped) {
                    Log.warn(LOG, "WAL roll failed: " + e.getMessage());
                }
            }
        }
    }

    /** Corresponds to HRegionServer.run(): reportForDuty, then tryRegionServerReport in a loop. */
    private void reportLoop() {
        RegionServerStatusService master = null;
        boolean registered = false;
        boolean warned = false;
        while (!stopped) {
            try {
                if (master == null) {
                    master = (RegionServerStatusService) LocateRegistry.getRegistry(HBaseConfiguration.HOSTNAME,
                            HBaseConfiguration.MASTER_PORT).lookup(RegionServerStatusService.NAME);
                }
                if (!registered) {
                    master.regionServerStartup(serverName);
                    registered = true;
                    Log.info(LOG, "reportForDuty to master=" + HBaseConfiguration.HOSTNAME + ":"
                            + HBaseConfiguration.MASTER_PORT + " with " + serverName);
                }
                master.regionServerReport(serverName, getRegionLoads());
                warned = false;
            } catch (YouAreDeadException e) {
                abort("Master rejected our report: " + e.getMessage());
                return;
            } catch (IOException | NotBoundException e) {
                master = null;
                if (!warned) {
                    Log.warn(LOG, serverName + ": master not reachable (" + e.getMessage() + "), retrying");
                    warned = true;
                }
            }
            rpcServices.expireScanners();
            try {
                Thread.sleep(HBaseConfiguration.MSG_INTERVAL);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private List<RegionServerStatusService.RegionLoad> getRegionLoads() {
        List<RegionServerStatusService.RegionLoad> r = new ArrayList<>();
        for (HRegion region : onlineRegions.values()) {
            int files = 0;
            long size = 0;
            for (HStore s : region.getStores()) {
                files += s.getStorefiles().size();
                size += s.getStorefilesSize();
            }
            r.add(new RegionServerStatusService.RegionLoad(region.getRegionInfo().getEncodedName(),
                    region.getMemStoreDataSize(), files, size, region.getReadRequestsCount(),
                    region.getWriteRequestsCount()));
        }
        return r;
    }

    // ----------------------------------------------------------- regions
    long openRegion(RegionInfo info, TableDescriptor htd) throws IOException {
        checkNotStopped();
        HRegion existing = onlineRegions.get(info.getEncodedName());
        if (existing != null) {
            return existing.getOpenSeqNum();
        }
        Log.info(LOG, serverName + ": opening region " + info.getRegionNameAsString());
        HRegion r = HRegion.openHRegion(info, htd, fs, wal, blockCache, this);
        onlineRegions.put(info.getEncodedName(), r);
        return r.getOpenSeqNum();
    }

    void closeRegion(String encodedName) throws IOException {
        HRegion r = onlineRegions.get(encodedName);
        if (r == null) {
            throw new NotServingRegionException("Region " + encodedName + " is not online on " + serverName);
        }
        r.close(false);
        onlineRegions.remove(encodedName);
    }

    public HRegion getRegion(String encodedName) throws NotServingRegionException {
        HRegion r = onlineRegions.get(encodedName);
        if (r == null || stopped) {
            throw new NotServingRegionException("Region " + encodedName + " is not online on " + serverName);
        }
        return r;
    }

    public Collection<HRegion> getOnlineRegions() {
        return onlineRegions.values();
    }

    void requestFlush(HRegion region, String why) {
        cacheFlusher.requestFlush(region, why);
    }

    void requestCompaction(HRegion region, HStore store, String why) {
        compactSplitThread.requestCompaction(region, store, why);
    }

    synchronized void addCompactionRecord(CompactionRecord r) {
        compactionHistory.addFirst(r);
        while (compactionHistory.size() > COMPACTION_HISTORY_SIZE) {
            compactionHistory.removeLast();
        }
    }

    synchronized List<CompactionRecord> getCompactionHistory() {
        return new ArrayList<>(compactionHistory);
    }

    private void checkNotStopped() throws IOException {
        if (stopped) {
            throw new IOException("Server " + serverName + " is stopping");
        }
    }

    // --------------------------------------------------------- shutdown
    /**
     * Crash simulation (corresponds to abort()): no flush, no WAL close, HDFS client cut off
     * abruptly - the WAL stays "under construction" with an open lease. The MemStores are lost;
     * their content exists only in the WAL now.
     */
    public void abort(String reason) {
        if (stopped) {
            return;
        }
        aborted = true;
        stopped = true;
        Log.warn(LOG, "***** ABORTING region server " + serverName + ": " + reason + " *****");
        unexportRpc();
        fs.abort();
        stopThreads();
        for (HRegion r : onlineRegions.values()) {
            try {
                r.close(true);
            } catch (IOException ignored) {
                // crash: nothing to save
            }
        }
        onlineRegions.clear();
    }

    /** Clean shutdown: close (= flush) all regions, close the WAL (-&gt; oldWALs). */
    public void stop(String reason) {
        if (stopped) {
            return;
        }
        Log.info(LOG, "Stopping " + serverName + ": " + reason);
        for (HRegion r : onlineRegions.values()) {
            try {
                r.close(false);
            } catch (IOException e) {
                Log.warn(LOG, "Failed to close " + r.getRegionInfo().getShortName() + ": " + e.getMessage());
            }
        }
        onlineRegions.clear();
        stopped = true;
        stopThreads();
        try {
            wal.close();
            fs.close();
        } catch (IOException e) {
            Log.warn(LOG, "Error closing WAL/HDFS: " + e.getMessage());
        }
        unexportRpc();
    }

    private void stopThreads() {
        cacheFlusher.shutdown();
        compactSplitThread.shutdown();
        walRoller.interrupt();
        reportThread.interrupt();
    }

    private void unexportRpc() {
        try {
            UnicastRemoteObject.unexportObject(rpcServices, true);
            UnicastRemoteObject.unexportObject(registry, true);
        } catch (IOException ignored) {
            // already unexported
        }
    }

    public ServerName getServerName() {
        return serverName;
    }

    public FSHLog getWAL() {
        return wal;
    }

    public LruBlockCache getBlockCache() {
        return blockCache;
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isAborted() {
        return aborted;
    }

    // ---------------------------------------------------------- metasave
    /** EduDFSBase: everything the RegionServer holds, readable (like -metasave in EduDFS). */
    public String metaSave() {
        StringBuilder sb = new StringBuilder();
        sb.append("RegionServer ").append(serverName).append(stopped ? "  (STOPPED)" : "").append('\n');
        sb.append("  WAL      ").append(wal.getCurrentFileName()).append('\n');
        sb.append(String.format("           %d entries, %d bytes (roll at %d bytes)%n", wal.getCurrentEntries(),
                wal.getCurrentFileLength(), wal.getLogrollSize()));
        appendLocalBlockPaths(sb, wal.getCurrentFileName(), "           ");
        for (String s : wal.describeRolledWALs()) {
            sb.append("           rolled: ").append(s).append('\n');
        }
        if (!wal.getArchived().isEmpty()) {
            sb.append("           archived to oldWALs: ").append(wal.getArchived().size()).append(" file(s)\n");
        }
        sb.append("  BlockCache ").append(blockCache).append('\n');
        List<HRegion> regions = new ArrayList<>(onlineRegions.values());
        regions.sort(Comparator.comparing((HRegion r) -> r.getRegionInfo().getTable())
                .thenComparing(r -> r.getRegionInfo().getStartKey(), Bytes::compareTo));
        sb.append("  Regions (").append(regions.size()).append("):\n");
        for (HRegion r : regions) {
            RegionInfo ri = r.getRegionInfo();
            sb.append(String.format("  %s  ['%s', '%s')%n", ri.getRegionNameAsString(),
                    Bytes.toStringBinary(ri.getStartKey()), Bytes.toStringBinary(ri.getEndKey())));
            sb.append(String.format("      openSeqNum=%d readPoint=%d lastFlushSeqId=%d reads=%d writes=%d%n",
                    r.getOpenSeqNum(), r.getReadPoint(), r.getLastFlushSeqId(), r.getReadRequestsCount(),
                    r.getWriteRequestsCount()));
            for (HStore s : r.getStores()) {
                DefaultMemStore ms = s.getMemStore();
                sb.append(String.format("      store %s: MemStore %d cells, %d bytes (flush at %d)%s%n",
                        s.getColumnFamilyName(), ms.getActiveCellsCount(), ms.getActiveDataSize(),
                        HBaseConfiguration.MEMSTORE_FLUSH_SIZE,
                        ms.getSnapshotCellsCount() > 0 ? ", snapshot " + ms.getSnapshotCellsCount() + " cells" : ""));
                int shown = 0;
                for (Cell c : ms.getActiveCells()) {
                    if (shown++ == 5) {
                        sb.append("          ... (").append(ms.getActiveCellsCount() - 5).append(" more)\n");
                        break;
                    }
                    sb.append("          ").append(KeyValueCodec.encode(c)).append('\n');
                }
                List<HStoreFile> files = s.getStorefiles();
                sb.append(String.format("        HFiles (%d)%s%n", files.size(), files.isEmpty() ? ": -"
                        : " in " + s.getRegionFileSystem().getStoreDir(s.getColumnFamilyName()) + "/:"));
                if (!files.isEmpty()) {
                    sb.append(String.format("          %-34s %7s %6s %6s %6s %6s %-6s %-22s %s%n", "name", "bytes",
                            "cells", "blocks", "levels", "seqid", "bloom", "bloom req/neg/fp", "created by"));
                }
                for (HStoreFile f : files) {
                    StoreFileReader rd = f.getReader();
                    int blocks;
                    try {
                        blocks = rd.getHFileReader().getDataBlockCount();
                    } catch (IOException e) {
                        blocks = -1;
                    }
                    sb.append(String.format("          %-34s %7d %6d %6d %6d %6d %-6s %-22s %s%n", f.getName(),
                            f.length(), rd.getEntries(), blocks,
                            rd.getHFileReader().getTrailer().getNumDataIndexLevels(), f.getMaxSequenceId(),
                            rd.getBloomFilterType(), rd.getBloomRequests() + "/" + rd.getBloomNegatives() + "/"
                                    + rd.getBloomFalsePositives(), f.getCreatedBy()));
                    appendLocalBlockPaths(sb, f.getPath(), "            ");
                }
            }
        }
        return sb.toString();
    }

    /**
     * EduDFSBase: the replicas of the file's HDFS blocks as files on the DataNodes' disks, ready for cat.
     * HBase itself only knows the HDFS path; the DataNodes are asked via ClientDatanodeProtocol.
     */
    private void appendLocalBlockPaths(StringBuilder sb, String path, String indent) {
        try {
            for (BlockLocalPathInfo info : fs.getClient().getBlockLocalPaths(path)) {
                sb.append(indent).append("replica ").append(info.getBlockPath() != null ? info.getBlockPath()
                        : info.getBlock().getBlockName() + " (DataNode not reachable)").append('\n');
            }
        } catch (IOException e) {
            sb.append(indent).append("replicas: ").append(e.getMessage()).append('\n');
        }
    }

    /** Starts RegionServer i (port hbase.regionserver.port + i) as a separate process. */
    public static void main(String[] args) throws Exception {
        int i = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        HRegionServer rs = new HRegionServer(HBaseConfiguration.REGIONSERVER_PORT + i);
        rs.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> rs.stop("shutdown hook")));
        Thread.currentThread().join();
    }
}
