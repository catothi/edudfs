package edudfsbase.client;

import edudfsbase.HConstants;
import edudfsbase.regionserver.BloomType;
import edudfsbase.util.Bytes;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hbase.client.ColumnFamilyDescriptor: the settings of a column
 * family. They apply per family because each family is a store of its own with its own HFiles.
 * Created with {@link ColumnFamilyDescriptorBuilder}.
 */
public final class ColumnFamilyDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] name;
    private final int maxVersions;
    private final BloomType bloomType;
    private final int blocksize;
    private final long timeToLive;
    private final boolean blockCacheEnabled;

    ColumnFamilyDescriptor(byte[] name, int maxVersions, BloomType bloomType, int blocksize, long timeToLive,
                           boolean blockCacheEnabled) {
        this.name = name;
        this.maxVersions = maxVersions;
        this.bloomType = bloomType;
        this.blocksize = blocksize;
        this.timeToLive = timeToLive;
        this.blockCacheEnabled = blockCacheEnabled;
    }

    public byte[] getName() {
        return name;
    }

    public String getNameAsString() {
        return Bytes.toString(name);
    }

    /** VERSIONS: how many versions per column are kept (original default: 1). */
    public int getMaxVersions() {
        return maxVersions;
    }

    /** BLOOMFILTER (original default: ROW). */
    public BloomType getBloomFilterType() {
        return bloomType;
    }

    /** BLOCKSIZE: target size of a data block in the HFile. */
    public int getBlocksize() {
        return blocksize;
    }

    /** TTL in seconds; cells older than this are invisible and are dropped by a major compaction. */
    public long getTimeToLive() {
        return timeToLive;
    }

    public boolean isBlockCacheEnabled() {
        return blockCacheEnabled;
    }

    Map<String, String> toMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("NAME", getNameAsString());
        m.put("BLOOMFILTER", bloomType.name());
        m.put("VERSIONS", String.valueOf(maxVersions));
        m.put("TTL", timeToLive == HConstants.FOREVER ? "FOREVER" : String.valueOf(timeToLive));
        m.put("BLOCKSIZE", String.valueOf(blocksize));
        m.put("BLOCKCACHE", String.valueOf(blockCacheEnabled));
        return m;
    }

    static ColumnFamilyDescriptor fromMap(Map<String, String> m) {
        ColumnFamilyDescriptorBuilder b = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(m.get("NAME")));
        if (m.containsKey("BLOOMFILTER")) {
            b.setBloomFilterType(BloomType.valueOf(m.get("BLOOMFILTER").toUpperCase()));
        }
        if (m.containsKey("VERSIONS")) {
            b.setMaxVersions(Integer.parseInt(m.get("VERSIONS")));
        }
        if (m.containsKey("TTL") && !m.get("TTL").equalsIgnoreCase("FOREVER")) {
            b.setTimeToLive(Long.parseLong(m.get("TTL")));
        }
        if (m.containsKey("BLOCKSIZE")) {
            b.setBlocksize(Integer.parseInt(m.get("BLOCKSIZE")));
        }
        if (m.containsKey("BLOCKCACHE")) {
            b.setBlockCacheEnabled(Boolean.parseBoolean(m.get("BLOCKCACHE")));
        }
        return b.build();
    }

    /** Format of the shell (describe): {NAME =&gt; 'cf', BLOOMFILTER =&gt; 'ROW', ...}. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (Map.Entry<String, String> e : toMap().entrySet()) {
            sb.append(sb.length() > 1 ? ", " : "").append(e.getKey()).append(" => '").append(e.getValue()).append("'");
        }
        return sb.append('}').toString();
    }
}
