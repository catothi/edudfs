package edudfsbase.client;

import edudfsbase.DoNotRetryIOException;
import edudfsbase.HBaseConfiguration;
import edudfsbase.HConstants;
import edudfsbase.HRegionLocation;
import edudfsbase.NotServingRegionException;
import edudfsbase.ServerName;
import edudfsbase.TableName;
import edudfsbase.ipc.AdminService;
import edudfsbase.ipc.ClientService;
import edudfsbase.ipc.MasterService;
import edudfsbase.util.Bytes;
import edudfs.common.Log;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Connection (ConnectionImplementation): the client's
 * connection to the cluster. Heavyweight - create once, share, close at the end.
 *
 * <b>Region location cache</b> (MetaCache): for each table the client remembers which RegionServer
 * serves which key range. A Get/Put goes DIRECTLY to that RegionServer. Only if it fails
 * (NotServingRegionException, connection error - e.g. because the region has moved after a
 * crash) does the client forget the entry, wait (hbase.client.pause x RETRY_BACKOFF[attempt]) and
 * ask again. After hbase.client.retries.number attempts: RetriesExhaustedException.
 */
public class Connection implements Closeable {

    private MasterService master;
    private final Map<ServerName, ClientService> clientStubs = new ConcurrentHashMap<>();
    private final Map<ServerName, AdminService> adminStubs = new ConcurrentHashMap<>();
    private final Map<TableName, List<HRegionLocation>> metaCache = new ConcurrentHashMap<>();
    private volatile boolean closed;

    Connection() {
    }

    public Table getTable(TableName tableName) {
        return new Table(this, tableName);
    }

    public Admin getAdmin() {
        return new Admin(this);
    }

    // ---------------------------------------------------------------- stubs
    synchronized MasterService getMaster() throws IOException {
        if (master == null) {
            try {
                master = (MasterService) LocateRegistry.getRegistry(HBaseConfiguration.HOSTNAME,
                        HBaseConfiguration.MASTER_PORT).lookup(MasterService.NAME);
            } catch (NotBoundException e) {
                throw new IOException(e);
            }
        }
        return master;
    }

    /**
     * EduDFSBase: one attempt to reach the master, without retries. The shell calls this at startup so
     * that a missing cluster is reported at once instead of after minutes of silent retries.
     */
    public void checkMasterReachable() throws IOException {
        try {
            getMaster();
        } catch (IOException e) {
            resetMaster();
            throw new IOException("Cannot reach the HMaster at " + HBaseConfiguration.HOSTNAME + ":"
                    + HBaseConfiguration.MASTER_PORT + " (" + e.getClass().getSimpleName() + ")", e);
        }
    }

    synchronized void resetMaster() {
        master = null;
    }

    ClientService getClient(ServerName sn) throws IOException {
        ClientService s = clientStubs.get(sn);
        if (s == null) {
            try {
                s = (ClientService) LocateRegistry.getRegistry(sn.getHostname(), sn.getPort())
                        .lookup(ClientService.NAME);
            } catch (NotBoundException e) {
                throw new IOException(e);
            }
            clientStubs.put(sn, s);
        }
        return s;
    }

    AdminService getAdmin(ServerName sn) throws IOException {
        AdminService s = adminStubs.get(sn);
        if (s == null) {
            try {
                s = (AdminService) LocateRegistry.getRegistry(sn.getHostname(), sn.getPort()).lookup(AdminService.NAME);
            } catch (NotBoundException e) {
                throw new IOException(e);
            }
            adminStubs.put(sn, s);
        }
        return s;
    }

    // -------------------------------------------------------- region lookup
    /** All regions of the table (from the cache or from the master). */
    List<HRegionLocation> locateRegions(TableName table, boolean reload) throws IOException {
        List<HRegionLocation> locs = reload ? null : metaCache.get(table);
        if (locs == null) {
            locs = withMasterRetries(() -> getMaster().getTableRegions(table));
            metaCache.put(table, locs);
        }
        return locs;
    }

    /** The region that contains row. */
    HRegionLocation locateRegion(TableName table, byte[] row, boolean reload) throws IOException {
        for (HRegionLocation loc : locateRegions(table, reload)) {
            if (loc.getRegion().containsRow(row)) {
                return loc;
            }
        }
        throw new IOException("No region found for row " + Bytes.toStringBinary(row) + " in " + table);
    }

    void clearRegionCache(TableName table) {
        metaCache.remove(table);
    }

    // --------------------------------------------------------------- retries
    interface Callable<T> {
        T call() throws IOException;
    }

    interface RegionCallable<T> {
        T call(ClientService rs, HRegionLocation loc) throws IOException;
    }

    /** Corresponds to RpcRetryingCaller: locate region, call, on error forget the location and retry. */
    <T> T callWithRetries(TableName table, byte[] row, RegionCallable<T> callable) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < HBaseConfiguration.CLIENT_RETRIES; attempt++) {
            checkClosed();
            HRegionLocation loc = null;
            try {
                loc = locateRegion(table, row, attempt > 0);
                if (loc.getServerName() == null) {
                    throw new NotServingRegionException("Region " + loc.getRegion().getShortName()
                            + " is not online (in transition)");
                }
                return callable.call(getClient(loc.getServerName()), loc);
            } catch (DoNotRetryIOException | RetriesExhaustedException e) {
                // RetriesExhaustedException: the master lookup already retried; retrying it again here
                // would multiply the waiting time (15 x 15 attempts)
                throw e;
            } catch (IOException e) {
                last = e;
                clearRegionCache(table);
                if (loc != null && loc.getServerName() != null) {
                    clientStubs.remove(loc.getServerName());
                }
                if (attempt == 0 || attempt % 3 == 2) {
                    Log.info("RpcRetryingCaller", "Call to " + (loc == null ? table : loc.getServerName())
                            + " failed (" + e.getClass().getSimpleName() + "), tries=" + (attempt + 1)
                            + ", retries=" + HBaseConfiguration.CLIENT_RETRIES + " - looking up region again");
                }
                sleep(attempt);
            }
        }
        throw new RetriesExhaustedException("Failed after attempts=" + HBaseConfiguration.CLIENT_RETRIES
                + ", last exception: " + last, last);
    }

    /** Master calls: retry while the master initializes or is (re)starting. */
    <T> T withMasterRetries(Callable<T> callable) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < HBaseConfiguration.CLIENT_RETRIES; attempt++) {
            checkClosed();
            try {
                return callable.call();
            } catch (DoNotRetryIOException e) {
                throw e;
            } catch (IOException e) {
                last = e;
                resetMaster();
                sleep(attempt);
            }
        }
        throw new RetriesExhaustedException("Master not available after attempts=" + HBaseConfiguration.CLIENT_RETRIES
                + ": " + last, last);
    }

    static void sleep(int attempt) throws InterruptedIOException {
        int i = Math.min(attempt, HConstants.RETRY_BACKOFF.length - 1);
        try {
            Thread.sleep(HBaseConfiguration.CLIENT_PAUSE * HConstants.RETRY_BACKOFF[i]);
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Connection is closed");
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
