package edubase;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hbase.NotServingRegionException: the RegionServer does not (or no longer)
 * serve the region. The client then asks the master for the new location and retries.
 */
public class NotServingRegionException extends IOException {

    private static final long serialVersionUID = 1L;

    public NotServingRegionException(String msg) {
        super(msg);
    }
}
