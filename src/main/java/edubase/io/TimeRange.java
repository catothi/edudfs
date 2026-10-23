package edubase.io;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hbase.io.TimeRange: [minStamp, maxStamp) - min inclusive,
 * max exclusive. Used by Get/Scan (only versions in the range) and by every HFile
 * (FileInfo TIMERANGE: a scan skips files whose range does not overlap).
 */
public final class TimeRange implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final TimeRange ALL_TIME = new TimeRange(0, Long.MAX_VALUE);

    private final long minStamp;
    private final long maxStamp;

    public TimeRange(long minStamp, long maxStamp) {
        if (maxStamp < minStamp) {
            throw new IllegalArgumentException("maxStamp is smaller than minStamp");
        }
        this.minStamp = minStamp;
        this.maxStamp = maxStamp;
    }

    public long getMin() {
        return minStamp;
    }

    public long getMax() {
        return maxStamp;
    }

    public boolean isAllTime() {
        return minStamp == 0 && maxStamp == Long.MAX_VALUE;
    }

    public boolean withinTimeRange(long ts) {
        return ts >= minStamp && ts < maxStamp;
    }

    /** Does [min, max] (both inclusive, as stored in an HFile) overlap this range? */
    public boolean includesTimeRange(long fileMin, long fileMax) {
        return fileMax >= minStamp && fileMin < maxStamp;
    }

    @Override
    public String toString() {
        return "[" + minStamp + ", " + maxStamp + ")";
    }
}
