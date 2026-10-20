package edudfs.common;

/** Corresponds to org.apache.hadoop.util.Time. */
public final class Time {

    private Time() {
    }

    /** Wall-clock time, e.g. for modification times in the namespace. */
    public static long now() {
        return System.currentTimeMillis();
    }

    /** Monotonic time for timeouts (leases, heartbeats) - does not jump when the clock is adjusted. */
    public static long monotonicNow() {
        return System.nanoTime() / 1_000_000L;
    }
}
