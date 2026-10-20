package edudfs.protocol;

import java.io.IOException;

/** The file is already being written by another client (the lease holder). (Name as in the original HDFS.) */
public class AlreadyBeingCreatedException extends IOException {
    private static final long serialVersionUID = 1L;

    public AlreadyBeingCreatedException(String msg) {
        super(msg);
    }
}
