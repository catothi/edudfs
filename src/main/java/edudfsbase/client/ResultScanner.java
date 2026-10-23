package edudfsbase.client;

import edudfsbase.HRegionLocation;
import edudfsbase.TableName;
import edudfsbase.ipc.ClientService;
import edudfsbase.ipc.ScanResponse;
import edudfsbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Corresponds to org.apache.hadoop.hbase.client.ResultScanner (ClientScanner): iterates over the
 * rows of a scan - region after region in key order.
 *
 * Per region: openScanner on its RegionServer, then next(caching) as often as needed, closeScanner.
 * If the RegionServer fails in between (or the region moves), the scanner is reopened
 * BEHIND the last row received - the client sees no row twice.
 */
public class ResultScanner implements Closeable, Iterable<Result> {

    private final Connection connection;
    private final TableName table;
    private final Scan scan;
    private final Deque<Result> cache = new ArrayDeque<>();
    private byte[] nextStartRow;
    private byte[] lastRow;
    private ClientService currentServer;
    private long scannerId = -1;
    private byte[] currentRegionEnd;
    private boolean exhausted;
    private int returned;

    ResultScanner(Connection connection, TableName table, Scan scan) {
        this.connection = connection;
        this.table = table;
        this.scan = scan;
        this.nextStartRow = scan.getStartRow();
    }

    /** The next row, null at the end. */
    public Result next() throws IOException {
        while (cache.isEmpty() && !exhausted) {
            loadCache();
        }
        if (cache.isEmpty() || (scan.getLimit() > 0 && returned >= scan.getLimit())) {
            close();
            return null;
        }
        Result r = cache.poll();
        lastRow = r.getRow();
        returned++;
        return r;
    }

    private void loadCache() throws IOException {
        if (scannerId < 0 && !openNextRegion()) {
            exhausted = true;
            return;
        }
        long id = scannerId;
        try {
            ScanResponse resp = currentServer.next(id, scan.getCaching());
            cache.addAll(resp.results());
            if (!resp.moreResultsInRegion()) {
                closeCurrent();
                if (currentRegionEnd.length == 0 || pastStopRow(currentRegionEnd)) {
                    exhausted = true;
                } else {
                    nextStartRow = currentRegionEnd;
                }
            }
        } catch (IOException e) {
            // RegionServer gone or region moved: reopen after the last row received
            scannerId = -1;
            connection.clearRegionCache(table);
            if (lastRow != null) {
                nextStartRow = Bytes.add(lastRow, new byte[]{0});
            }
            Connection.sleep(1);
        }
    }

    private boolean pastStopRow(byte[] row) {
        byte[] stop = scan.getStopRow();
        return stop.length > 0 && Bytes.compareTo(row, stop) >= 0;
    }

    private boolean openNextRegion() throws IOException {
        if (pastStopRow(nextStartRow) && !scan.includeStopRow()) {
            return false;
        }
        Scan s = scan.copyWithStartRow(nextStartRow);
        long[] id = new long[1];
        HRegionLocation loc = connection.callWithRetries(table, nextStartRow, (rs, l) -> {
            id[0] = rs.openScanner(l.getRegion().getEncodedName(), s);
            currentServer = rs;
            return l;
        });
        scannerId = id[0];
        currentRegionEnd = loc.getRegion().getEndKey();
        return true;
    }

    private void closeCurrent() {
        if (scannerId >= 0) {
            try {
                currentServer.closeScanner(scannerId);
            } catch (IOException ignored) {
                // the server forgets it anyway (lease)
            }
            scannerId = -1;
        }
    }

    @Override
    public void close() {
        closeCurrent();
        exhausted = true;
        cache.clear();
    }

    @Override
    public Iterator<Result> iterator() {
        return new Iterator<>() {
            private Result next;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    try {
                        next = ResultScanner.this.next();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                return next != null;
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Result r = next;
                next = null;
                return r;
            }
        };
    }
}
