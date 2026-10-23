package edudfsbase.client;

import edudfsbase.TableName;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.client.Table (HTable): the data operations on a table.
 * Every call goes to the RegionServer that serves the row's region (Connection.callWithRetries).
 */
public class Table implements Closeable {

    private final Connection connection;
    private final TableName tableName;

    Table(Connection connection, TableName tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    public TableName getName() {
        return tableName;
    }

    public void put(Put put) throws IOException {
        connection.callWithRetries(tableName, put.getRow(), (rs, loc) -> {
            rs.mutate(loc.getRegion().getEncodedName(), put);
            return null;
        });
    }

    public void put(List<Put> puts) throws IOException {
        for (Put p : puts) {
            put(p);
        }
    }

    public void delete(Delete delete) throws IOException {
        connection.callWithRetries(tableName, delete.getRow(), (rs, loc) -> {
            rs.mutate(loc.getRegion().getEncodedName(), delete);
            return null;
        });
    }

    public Result get(Get get) throws IOException {
        return connection.callWithRetries(tableName, get.getRow(),
                (rs, loc) -> rs.get(loc.getRegion().getEncodedName(), get));
    }

    public boolean exists(Get get) throws IOException {
        return !get(get).isEmpty();
    }

    public ResultScanner getScanner(Scan scan) {
        return new ResultScanner(connection, tableName, scan);
    }

    @Override
    public void close() {
        // nothing to release: the connection holds the resources
    }
}
