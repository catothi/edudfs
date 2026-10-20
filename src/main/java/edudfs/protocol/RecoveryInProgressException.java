package edudfs.protocol;

import java.io.IOException;

/** Lease recovery for the file is in progress - try again later. (Name as in the original HDFS.) */
public class RecoveryInProgressException extends IOException {
    private static final long serialVersionUID = 1L;

    public RecoveryInProgressException(String msg) {
        super(msg);
    }
}
