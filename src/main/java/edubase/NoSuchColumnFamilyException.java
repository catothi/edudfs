package edubase;

/** Corresponds to org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException. */
public class NoSuchColumnFamilyException extends DoNotRetryIOException {

    private static final long serialVersionUID = 1L;

    public NoSuchColumnFamilyException(String msg) {
        super(msg);
    }
}
