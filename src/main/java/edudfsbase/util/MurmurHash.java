package edudfsbase.util;

/**
 * Corresponds to org.apache.hadoop.hbase.util.MurmurHash (MurmurHash2, 32 bit).
 * The original default for Bloom filters ({@code hbase.hash.type = murmur}).
 *
 * Algorithm: MurmurHash2 by Austin Appleby, placed in the public domain
 * (https://github.com/aappleby/smhasher). The byte-wise Java formulation follows the widely used Java port
 * of this algorithm, which Apache Hadoop and Apache HBase also contain (Apache License 2.0). The result
 * must be bit-identical to theirs, which leaves little freedom in how the loop is written.
 */
public final class MurmurHash {

    private MurmurHash() {
    }

    public static int hash(byte[] data, int offset, int length, int seed) {
        final int m = 0x5bd1e995;
        final int r = 24;
        int h = seed ^ length;
        int len4 = length >> 2;
        for (int i = 0; i < len4; i++) {
            int i4 = offset + (i << 2);
            int k = data[i4 + 3];
            k = k << 8;
            k = k | (data[i4 + 2] & 0xff);
            k = k << 8;
            k = k | (data[i4 + 1] & 0xff);
            k = k << 8;
            k = k | (data[i4] & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        int lenM = len4 << 2;
        int left = length - lenM;
        int iM = offset + lenM;
        if (left != 0) {
            if (left >= 3) {
                h ^= data[iM + 2] << 16;
            }
            if (left >= 2) {
                h ^= data[iM + 1] << 8;
            }
            h ^= data[iM];
            h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }
}
