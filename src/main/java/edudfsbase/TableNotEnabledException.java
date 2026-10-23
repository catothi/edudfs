package edudfsbase;

/** Corresponds to org.apache.hadoop.hbase.TableNotEnabledException: access to a disabled table. */
public class TableNotEnabledException extends DoNotRetryIOException {

    private static final long serialVersionUID = 1L;

    public TableNotEnabledException(String msg) {
        super(msg);
    }
}
