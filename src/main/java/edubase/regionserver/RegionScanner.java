package edubase.regionserver;

import edubase.Cell;
import edubase.client.Scan;
import edubase.util.ReadTrace;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.RegionScanner (RegionScannerImpl): reads a region
 * row by row. Per row it collects the cells of all requested families - each family is a store of its
 * own with its own StoreScanner, so the rows of the stores are merged here.
 */
public class RegionScanner implements Closeable {

    private final HRegion region;
    private final Scan scan;
    private final List<StoreScanner> storeScanners;
    private final ReadTrace trace;
    private boolean done;

    RegionScanner(HRegion region, Scan scan, List<StoreScanner> storeScanners, ReadTrace trace) {
        this.region = region;
        this.scan = scan;
        this.storeScanners = storeScanners;
        this.trace = trace;
    }

    /**
     * Fills out with the cells of the next row (that has at least one visible cell).
     * @return true if more rows may follow
     */
    public boolean next(List<Cell> out, List<String> sources) throws IOException {
        while (!done) {
            byte[] row = null;
            for (StoreScanner s : storeScanners) {
                Cell c = s.peek();
                if (c != null && (row == null || HRegion.compareRows(c.getRow(), row) < 0)) {
                    row = c.getRow();
                }
            }
            if (row == null || isStopRow(row)) {
                done = true;
                return false;
            }
            int before = out.size();
            for (StoreScanner s : storeScanners) {
                Cell c = s.peek();
                if (c != null && HRegion.compareRows(c.getRow(), row) == 0) {
                    s.next(out, sources);
                }
            }
            if (out.size() > before) {
                return true;  // a row with visible cells; otherwise (everything deleted) go on to the next row
            }
        }
        return false;
    }

    private boolean isStopRow(byte[] row) {
        if (!region.getRegionInfo().containsRow(row)) {
            return true;
        }
        byte[] stop = scan.getStopRow();
        if (stop.length == 0) {
            return false;
        }
        int c = HRegion.compareRows(row, stop);
        return c > 0 || (c == 0 && !scan.includeStopRow());
    }

    public ReadTrace getTrace() {
        return trace;
    }

    public HRegion getRegion() {
        return region;
    }

    @Override
    public void close() {
        for (StoreScanner s : storeScanners) {
            s.close();
        }
    }
}
