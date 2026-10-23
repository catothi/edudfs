package edudfsbase;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.YouAreDeadException: the master has already declared this RegionServer
 * dead (and split its WALs). The RegionServer must stop at once - it no longer owns its regions.
 */
public class YouAreDeadException extends IOException {

    private static final long serialVersionUID = 1L;

    public YouAreDeadException(String msg) {
        super(msg);
    }
}
