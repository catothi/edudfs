package edubase;

/** Corresponds to org.apache.hadoop.hbase.TableNotFoundException. */
public class TableNotFoundException extends DoNotRetryIOException {

    private static final long serialVersionUID = 1L;

    public TableNotFoundException(String msg) {
        super(msg);
    }
}
