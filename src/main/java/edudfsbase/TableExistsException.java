package edudfsbase;

/** Corresponds to org.apache.hadoop.hbase.TableExistsException. */
public class TableExistsException extends DoNotRetryIOException {

    private static final long serialVersionUID = 1L;

    public TableExistsException(String msg) {
        super(msg);
    }
}
