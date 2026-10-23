package edudfsbase.util;

/**
 * Corresponds to org.apache.hadoop.hbase.util.BloomFilterChunk: a Bloom filter as a bit array.
 *
 * add(key): k positions are computed from two hash values (Kirsch/Mitzenmacher 2006, "Less Hashing,
 * Same Performance"; as in the original): {@code pos_i = (h1 + i * h2) mod bitSize} with h1 = murmur(key, 0) and
 * h2 = murmur(key, h1); all k bits are set.
 * contains(key): false if at least one of the k bits is 0 - then the key is DEFINITELY
 * not in the file. true means "maybe" (false positive possible).
 *
 * Simplification: the original splits the filter of an HFile into chunks of 128 KB
 * (CompoundBloomFilter) and can "fold" it afterwards (shrink it if fewer keys than
 * planned arrived). EduDFSBase writes exactly one chunk per HFile.
 */
public class BloomFilterChunk {

    private final byte[] bloom;
    private final long bitSize;
    private final int hashCount;
    private final long maxKeys;
    private long keyCount;

    public BloomFilterChunk(long maxKeys, double errorRate) {
        this.maxKeys = Math.max(1, maxKeys);
        long bits = Math.max(8, BloomFilterUtil.computeBitSize(this.maxKeys, errorRate));
        this.bloom = new byte[(int) ((bits + 7) / 8)];
        this.bitSize = bloom.length * 8L;
        this.hashCount = BloomFilterUtil.optimalFunctionCount(this.maxKeys, bitSize);
    }

    /** For reading: the chunk as stored in the HFile. */
    public BloomFilterChunk(byte[] bloom, int hashCount, long keyCount, long maxKeys) {
        this.bloom = bloom;
        this.bitSize = bloom.length * 8L;
        this.hashCount = hashCount;
        this.keyCount = keyCount;
        this.maxKeys = maxKeys;
    }

    public void add(byte[] key) {
        int hash1 = MurmurHash.hash(key, 0, key.length, 0);
        int hash2 = MurmurHash.hash(key, 0, key.length, hash1);
        for (int i = 0; i < hashCount; i++) {
            long pos = Math.abs((hash1 + i * (long) hash2) % bitSize);
            bloom[(int) (pos >> 3)] |= (byte) (1 << (pos & 7));
        }
        keyCount++;
    }

    public boolean contains(byte[] key) {
        int hash1 = MurmurHash.hash(key, 0, key.length, 0);
        int hash2 = MurmurHash.hash(key, 0, key.length, hash1);
        for (int i = 0; i < hashCount; i++) {
            long pos = Math.abs((hash1 + i * (long) hash2) % bitSize);
            if ((bloom[(int) (pos >> 3)] & (1 << (pos & 7))) == 0) {
                return false;
            }
        }
        return true;
    }

    public byte[] getBytes() {
        return bloom;
    }

    public long getBitSize() {
        return bitSize;
    }

    public int getHashCount() {
        return hashCount;
    }

    public long getKeyCount() {
        return keyCount;
    }

    public long getMaxKeys() {
        return maxKeys;
    }

    /** Share of bits set to 1 (edu). Near 50 % the filter is "optimally full". */
    public double fillRatio() {
        long ones = 0;
        for (byte b : bloom) {
            ones += Integer.bitCount(b & 0xff);
        }
        return (double) ones / bitSize;
    }

    /** False positive rate to expect with the keys actually added. */
    public double actualErrorRate() {
        return BloomFilterUtil.actualErrorRate(keyCount, bitSize, hashCount);
    }
}
