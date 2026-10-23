package edudfsbase.regionserver;

import edudfsbase.Cell;
import edudfsbase.HBaseConfiguration;
import edudfsbase.RegionInfo;
import edudfsbase.UnknownScannerException;
import edudfsbase.client.Get;
import edudfsbase.client.Mutation;
import edudfsbase.client.Result;
import edudfsbase.client.Scan;
import edudfsbase.client.TableDescriptor;
import edudfsbase.ipc.AdminService;
import edudfsbase.ipc.ClientService;
import edudfsbase.ipc.ScanResponse;
import edudfsbase.regionserver.compactions.CompactionRecord;
import edudfsbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.RSRpcServices: the RPC endpoints of the
 * RegionServer. Delegates to HRegion; additionally manages the open scanners of the clients
 * (with lease: an unused scanner is closed after hbase.client.scanner.timeout.period).
 */
public class RSRpcServices implements ClientService, AdminService {

    private final HRegionServer rs;
    private final AtomicLong nextScannerId = new AtomicLong(1);
    private final Map<Long, ScannerHolder> scanners = new ConcurrentHashMap<>();

    private static final class ScannerHolder {
        final RegionScanner scanner;
        final boolean showSource;
        volatile long lastAccess = EnvironmentEdgeManager.currentTime();
        boolean traceSent;

        ScannerHolder(RegionScanner scanner, boolean showSource) {
            this.scanner = scanner;
            this.showSource = showSource;
        }
    }

    RSRpcServices(HRegionServer rs) {
        this.rs = rs;
    }

    // ---------------------------------------------------- ClientService
    @Override
    public Result get(String encodedRegionName, Get get) throws IOException {
        return rs.getRegion(encodedRegionName).get(get);
    }

    @Override
    public void mutate(String encodedRegionName, Mutation mutation) throws IOException {
        rs.getRegion(encodedRegionName).batchMutate(mutation);
    }

    @Override
    public long openScanner(String encodedRegionName, Scan scan) throws IOException {
        RegionScanner s = rs.getRegion(encodedRegionName).getScanner(scan);
        long id = nextScannerId.getAndIncrement();
        scanners.put(id, new ScannerHolder(s, scan.isShowSource()));
        return id;
    }

    @Override
    public ScanResponse next(long scannerId, int nbRows) throws IOException {
        ScannerHolder h = scanners.get(scannerId);
        if (h == null) {
            throw new UnknownScannerException("Scanner " + scannerId + " is not known on " + rs.getServerName()
                    + " (wrong ID, lease expired after hbase.client.scanner.timeout.period, or server restarted)");
        }
        h.lastAccess = EnvironmentEdgeManager.currentTime();
        rs.getRegion(h.scanner.getRegion().getRegionInfo().getEncodedName());  // still online?
        List<Result> results = new ArrayList<>();
        boolean more = true;
        synchronized (h) {
            while (results.size() < nbRows && more) {
                List<Cell> cells = new ArrayList<>();
                List<String> sources = h.showSource ? new ArrayList<>() : null;
                more = h.scanner.next(cells, sources);
                if (!cells.isEmpty()) {
                    List<String> trace = null;
                    if (!h.traceSent && h.scanner.getTrace() != null) {
                        trace = h.scanner.getTrace().getLines();
                        h.traceSent = true;
                    }
                    results.add(new Result(cells, sources, trace));
                }
            }
        }
        return new ScanResponse(results, more);
    }

    @Override
    public void closeScanner(long scannerId) {
        ScannerHolder h = scanners.remove(scannerId);
        if (h != null) {
            h.scanner.close();
        }
    }

    /** Lease check of the scanners (called by the RegionServer's report thread). */
    void expireScanners() {
        long now = EnvironmentEdgeManager.currentTime();
        scanners.entrySet().removeIf(e -> {
            if (now - e.getValue().lastAccess > HBaseConfiguration.SCANNER_TIMEOUT) {
                e.getValue().scanner.close();
                return true;
            }
            return false;
        });
    }

    // ----------------------------------------------------- AdminService
    @Override
    public long openRegion(RegionInfo region, TableDescriptor htd) throws IOException {
        return rs.openRegion(region, htd);
    }

    @Override
    public void closeRegion(String encodedRegionName) throws IOException {
        rs.closeRegion(encodedRegionName);
    }

    @Override
    public String flushRegion(String encodedRegionName) throws IOException {
        return rs.getRegion(encodedRegionName).flush("flush requested");
    }

    @Override
    public List<CompactionRecord> compactRegion(String encodedRegionName, boolean major) throws IOException {
        List<CompactionRecord> r = rs.getRegion(encodedRegionName).compact(major);
        for (CompactionRecord c : r) {
            rs.addCompactionRecord(c);
        }
        return r;
    }

    @Override
    public List<RegionInfo> getOnlineRegions() {
        List<RegionInfo> r = new ArrayList<>();
        for (HRegion region : rs.getOnlineRegions()) {
            r.add(region.getRegionInfo());
        }
        return r;
    }

    @Override
    public String metaSave() {
        return rs.metaSave();
    }

    @Override
    public List<CompactionRecord> getCompactionHistory() {
        return rs.getCompactionHistory();
    }
}
