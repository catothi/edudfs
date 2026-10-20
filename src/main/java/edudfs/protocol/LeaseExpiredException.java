package edudfs.protocol;

import java.io.IOException;

/** The client does not (or no longer) hold the lease for the file. (Name as in the original HDFS.) */
public class LeaseExpiredException extends IOException {
    private static final long serialVersionUID = 1L;

    public LeaseExpiredException(String msg) {
        super(msg);
    }
}
