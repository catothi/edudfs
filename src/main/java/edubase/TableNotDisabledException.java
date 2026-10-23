package edubase;

/**
 * Corresponds to org.apache.hadoop.hbase.TableNotDisabledException: e.g. drop of a table that is still enabled.
 */
public class TableNotDisabledException extends DoNotRetryIOException {

    private static final long serialVersionUID = 1L;

    public TableNotDisabledException(String msg) {
        super(msg);
    }
}
