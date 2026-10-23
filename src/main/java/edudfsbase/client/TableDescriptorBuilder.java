package edudfsbase.client;

import edudfsbase.TableName;

import java.util.ArrayList;
import java.util.List;

/** Corresponds to org.apache.hadoop.hbase.client.TableDescriptorBuilder. */
public final class TableDescriptorBuilder {

    private final TableName tableName;
    private final List<ColumnFamilyDescriptor> families = new ArrayList<>();

    private TableDescriptorBuilder(TableName tableName) {
        this.tableName = tableName;
    }

    public static TableDescriptorBuilder newBuilder(TableName tableName) {
        return new TableDescriptorBuilder(tableName);
    }

    public TableDescriptorBuilder setColumnFamily(ColumnFamilyDescriptor family) {
        families.add(family);
        return this;
    }

    public TableDescriptor build() {
        if (families.isEmpty()) {
            throw new IllegalArgumentException("Table should have at least one column family.");
        }
        return new TableDescriptor(tableName, families);
    }
}
