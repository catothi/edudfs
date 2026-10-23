package edudfsbase.client;

import edudfsbase.TableName;
import edudfsbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Corresponds to org.apache.hadoop.hbase.client.TableDescriptor: name + column families of a table.
 * Only the families are fixed when a table is created - the columns (qualifiers) within a family
 * are free and can differ from row to row ("wide column store").
 *
 * Stored in HDFS as /hbase/data/default/&lt;table&gt;/.tabledesc/.tableinfo.0000000001.
 * Original: protobuf (binary); here readable text, one line per family.
 */
public final class TableDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableName tableName;
    /** Sorted by name, as the stores of a region. */
    private final Map<String, ColumnFamilyDescriptor> families;

    TableDescriptor(TableName tableName, Collection<ColumnFamilyDescriptor> families) {
        this.tableName = tableName;
        Map<String, ColumnFamilyDescriptor> m = new TreeMap<>();
        for (ColumnFamilyDescriptor f : families) {
            m.put(f.getNameAsString(), f);
        }
        this.families = Collections.unmodifiableMap(m);
    }

    public TableName getTableName() {
        return tableName;
    }

    public Collection<ColumnFamilyDescriptor> getColumnFamilies() {
        return families.values();
    }

    public ColumnFamilyDescriptor getColumnFamily(byte[] name) {
        return families.get(Bytes.toString(name));
    }

    public boolean hasColumnFamily(byte[] name) {
        return families.containsKey(Bytes.toString(name));
    }

    public String toFileContent() {
        StringBuilder sb = new StringBuilder("tableName=" + tableName + "\n");
        for (ColumnFamilyDescriptor f : families.values()) {
            StringBuilder line = new StringBuilder();
            for (Map.Entry<String, String> e : f.toMap().entrySet()) {
                line.append(line.length() > 0 ? "," : "").append(e.getKey()).append('=').append(e.getValue());
            }
            sb.append("family=").append(line).append('\n');
        }
        return sb.toString();
    }

    public static TableDescriptor parseFrom(String content) {
        TableName name = null;
        List<ColumnFamilyDescriptor> fams = new ArrayList<>();
        for (String line : content.split("\n")) {
            if (line.startsWith("tableName=")) {
                name = TableName.valueOf(line.substring("tableName=".length()));
            } else if (line.startsWith("family=")) {
                Map<String, String> m = new LinkedHashMap<>();
                for (String kv : line.substring("family=".length()).split(",")) {
                    int i = kv.indexOf('=');
                    m.put(kv.substring(0, i), kv.substring(i + 1));
                }
                fams.add(ColumnFamilyDescriptor.fromMap(m));
            }
        }
        return new TableDescriptor(name, fams);
    }

    /** Format of the shell: 't', {NAME =&gt; 'cf', ...}, {NAME =&gt; 'cf2', ...}. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("'" + tableName + "'");
        for (ColumnFamilyDescriptor f : families.values()) {
            sb.append(", ").append(f);
        }
        return sb.toString();
    }
}
