package edudfsbase.util;

/**
 * Corresponds to org.apache.hadoop.hbase.util.BloomFilterUtil: the formulas for sizing a Bloom filter
 * (Bloom 1970; the standard analysis, e.g. Broder/Mitzenmacher 2004 - see docs/EDUDFSBASE.md, References).
 *
 * <pre>
 *   n = number of keys, p = desired false positive rate
 *   bits      m = -n * ln(p) / (ln 2)^2          (e.g. n=1000, p=1 %  -&gt;  9586 bits = 1.2 KB)
 *   hashes    k = m / n * ln 2                   (rounded, here 7)
 *   real rate   = (1 - e^(-k*n/m))^k             (with the actual number of keys n)
 * </pre>
 */
public final class BloomFilterUtil {

    private static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

    private BloomFilterUtil() {
    }

    public static long computeBitSize(long maxKeys, double errorRate) {
        return (long) Math.ceil(maxKeys * (-Math.log(errorRate) / LOG2_SQUARED));
    }

    public static int optimalFunctionCount(long maxKeys, long bitSize) {
        long i = bitSize / maxKeys;
        double result = Math.ceil(Math.log(2) * i);
        return Math.max(1, (int) result);
    }

    /** Expected false positive rate for keyCount keys in bitSize bits with hashCount functions. */
    public static double actualErrorRate(long keyCount, long bitSize, int hashCount) {
        return Math.pow(1 - Math.exp(-(double) hashCount * keyCount / bitSize), hashCount);
    }
}
