package edubase;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.UnknownScannerException: the scanner ID is unknown to the
 * RegionServer (expired after hbase.client.scanner.timeout.period, or the server was restarted).
 * The client then reopens the scanner after the last row it received.
 */
public class UnknownScannerException extends IOException {

    private static final long serialVersionUID = 1L;

    public UnknownScannerException(String msg) {
        super(msg);
    }
}
