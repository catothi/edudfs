package edubase;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.PleaseHoldException: the master is still initializing
 * (waiting for RegionServers, splitting WALs, assigning regions). The client retries.
 */
public class PleaseHoldException extends IOException {

    private static final long serialVersionUID = 1L;

    public PleaseHoldException(String msg) {
        super(msg);
    }
}
