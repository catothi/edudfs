package edudfs.common;

import java.io.IOException;

/** Corresponds to org.apache.hadoop.fs.ChecksumException. */
public class ChecksumException extends IOException {
    private static final long serialVersionUID = 1L;

    public ChecksumException(String msg) {
        super(msg);
    }
}
