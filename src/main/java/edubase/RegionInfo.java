package edubase;

import edubase.util.Bytes;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hbase.client.RegionInfo (formerly HRegionInfo).
 *
 * A region is a contiguous key range [startKey, endKey) of a table. An empty startKey means
 * "from the beginning", an empty endKey "to the end". A new table has one region
 * [ , ) unless split keys are given when it is created.
 *
 * Names (as in the original):
 * <pre>
 *   region name:   &lt;table&gt;,&lt;startKey&gt;,&lt;regionId&gt;.&lt;encodedName&gt;.
 *                  e.g. t,g,1761213640000.3f2a…9c.
 *   encoded name:  MD5 hex of "&lt;table&gt;,&lt;startKey&gt;,&lt;regionId&gt;"       = directory name in HDFS
 * </pre>
 * The regionId is the creation time in ms.
 */
public final class RegionInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableName table;
    private final byte[] startKey;
    private final byte[] endKey;
    private final long regionId;
    private final String encodedName;

    public RegionInfo(TableName table, byte[] startKey, byte[] endKey, long regionId) {
        this.table = table;
        this.startKey = startKey == null ? HConstants.EMPTY_START_ROW : startKey;
        this.endKey = endKey == null ? HConstants.EMPTY_END_ROW : endKey;
        this.regionId = regionId;
        this.encodedName = encodeRegionName(table + "," + Bytes.toStringBinary(this.startKey) + "," + regionId);
    }

    private static String encodeRegionName(String s) {
        try {
            return Bytes.toHex(MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public TableName getTable() {
        return table;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public long getRegionId() {
        return regionId;
    }

    public String getEncodedName() {
        return encodedName;
    }

    public String getRegionNameAsString() {
        return table + "," + Bytes.toStringBinary(startKey) + "," + regionId + "." + encodedName + ".";
    }

    /** Does row lie in [startKey, endKey)? */
    public boolean containsRow(byte[] row) {
        return Bytes.compareTo(row, startKey) >= 0
                && (endKey.length == 0 || Bytes.compareTo(row, endKey) < 0);
    }

    /** Shortened encoded name for tables and logs (first 8 hex characters). */
    public String getShortName() {
        return encodedName.substring(0, 8);
    }

    // ------------------------------------------------------ .regioninfo file
    /**
     * Content of the file &lt;region dir&gt;/.regioninfo. Original: magic "PBUF" followed by a
     * protobuf RegionInfo (binary); here readable key=value lines.
     */
    public String toFileContent() {
        return "tableName=" + table + "\nstartKey=" + Bytes.toStringBinary(startKey, " =")
                + "\nendKey=" + Bytes.toStringBinary(endKey, " =") + "\nregionId=" + regionId
                + "\nencodedName=" + encodedName + "\n";
    }

    public static RegionInfo parseFrom(String content) {
        Map<String, String> m = new LinkedHashMap<>();
        for (String line : content.split("\n")) {
            int i = line.indexOf('=');
            if (i > 0) {
                m.put(line.substring(0, i), line.substring(i + 1));
            }
        }
        return new RegionInfo(TableName.valueOf(m.get("tableName")), Bytes.toBytesBinary(m.get("startKey")),
                Bytes.toBytesBinary(m.get("endKey")), Long.parseLong(m.get("regionId")));
    }

    /** Same format as the original. */
    @Override
    public String toString() {
        return "{ENCODED => " + encodedName + ", NAME => '" + getRegionNameAsString() + "', STARTKEY => '"
                + Bytes.toStringBinary(startKey) + "', ENDKEY => '" + Bytes.toStringBinary(endKey) + "'}";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RegionInfo r && r.encodedName.equals(encodedName);
    }

    @Override
    public int hashCode() {
        return encodedName.hashCode();
    }
}
