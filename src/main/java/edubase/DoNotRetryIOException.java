package edubase;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.DoNotRetryIOException: the client must not retry (the error would not
 * go away).
 */
public class DoNotRetryIOException extends IOException {

    private static final long serialVersionUID = 1L;

    public DoNotRetryIOException(String msg) {
        super(msg);
    }
}
