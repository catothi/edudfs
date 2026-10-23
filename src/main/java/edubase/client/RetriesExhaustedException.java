package edubase.client;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.client.RetriesExhaustedException: all hbase.client.retries.number
 * attempts failed.
 */
public class RetriesExhaustedException extends IOException {

    private static final long serialVersionUID = 1L;

    public RetriesExhaustedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
