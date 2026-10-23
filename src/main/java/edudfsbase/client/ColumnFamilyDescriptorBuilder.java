package edudfsbase.client;

import edudfsbase.HConstants;
import edudfsbase.regionserver.BloomType;
import edudfsbase.util.Bytes;

/** Corresponds to org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder. */
public final class ColumnFamilyDescriptorBuilder {

    public static final int DEFAULT_VERSIONS = 1;
    public static final BloomType DEFAULT_BLOOMFILTER = BloomType.ROW;
    /** Original: 64 KB. Smaller here so that a few rows already fill several blocks. */
    public static final int DEFAULT_BLOCKSIZE = 1024;

    private final byte[] name;
    private int maxVersions = DEFAULT_VERSIONS;
    private BloomType bloomType = DEFAULT_BLOOMFILTER;
    private int blocksize = DEFAULT_BLOCKSIZE;
    private long timeToLive = HConstants.FOREVER;
    private boolean blockCacheEnabled = true;

    private ColumnFamilyDescriptorBuilder(byte[] name) {
        this.name = name;
    }

    public static ColumnFamilyDescriptorBuilder newBuilder(byte[] name) {
        return new ColumnFamilyDescriptorBuilder(name);
    }

    public static ColumnFamilyDescriptor of(String name) {
        return newBuilder(Bytes.toBytes(name)).build();
    }

    public ColumnFamilyDescriptorBuilder setMaxVersions(int maxVersions) {
        this.maxVersions = maxVersions;
        return this;
    }

    public ColumnFamilyDescriptorBuilder setBloomFilterType(BloomType bloomType) {
        this.bloomType = bloomType;
        return this;
    }

    public ColumnFamilyDescriptorBuilder setBlocksize(int blocksize) {
        this.blocksize = blocksize;
        return this;
    }

    public ColumnFamilyDescriptorBuilder setTimeToLive(long seconds) {
        this.timeToLive = seconds;
        return this;
    }

    public ColumnFamilyDescriptorBuilder setBlockCacheEnabled(boolean enabled) {
        this.blockCacheEnabled = enabled;
        return this;
    }

    public ColumnFamilyDescriptor build() {
        if (!Bytes.toString(name).matches("[a-zA-Z0-9_][a-zA-Z0-9_.-]*")) {
            throw new IllegalArgumentException("Illegal column family name: '" + Bytes.toStringBinary(name) + "'");
        }
        return new ColumnFamilyDescriptor(name, maxVersions, bloomType, blocksize, timeToLive, blockCacheEnabled);
    }
}
