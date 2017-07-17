package com.github.seijuro.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by seijuro
 */
public class HBaseHelper {
    private final Configuration config;
    private final User user;
    private final Connection conn;

    /**
     * C'tor
     *
     * @param $config
     */
    public HBaseHelper(Configuration $config, User user) throws IOException {
        this.config = $config;
        this.user = user;

        this.conn = ConnectionFactory.createConnection(this.config);
    }

    public HBaseHelper(Configuration $config) throws IOException {
        this($config, null);
    }

    /**
     * scan
     *
     * @param connection
     * @param tablename
     * @param column
     * @return
     * @throws IOException
     */
    public Map<byte[], byte[]> scan(Connection connection, String tablename, HBaseColumn column) throws IOException {
        Connection conn = connection;

        if (conn == null) {
            conn = ConnectionFactory.createConnection(this.config, this.user);
        }

        assert (conn != null);

        Table table = conn.getTable(TableName.valueOf(tablename));

        ResultScanner scanner = table.getScanner(new Scan());
        Map<byte[], byte[]> retMap = new HashMap<>();

        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            byte[] key = result.getRow();
            byte[] value = result.getValue(column.familtyBytes(), column.qualifierBytes());

            retMap.put(key, value);
        }

        if (connection == null) {
            conn.close();
        }

        return retMap;
    }

    public Map<byte[], byte[]> scan(String tablename, HBaseColumn column) throws IOException {
        return scan(ConnectionFactory.createConnection(this.config), tablename, column);
    }

    /**
     * put all records
     *
     * @param connection
     * @param tablename
     * @param records
     * @throws IOException
     */
    public void puts(Connection connection, String tablename, Map<byte[], List<HbaseColumnValue>> records) throws IOException {
        Connection conn = connection;

        if (conn == null) {
            conn = ConnectionFactory.createConnection(this.config, this.user);
        }

        assert (conn != null);

        Table table = conn.getTable(TableName.valueOf(tablename));

        for (Map.Entry<byte[], List<HbaseColumnValue>> entry : records.entrySet()) {
            Put put = new Put(entry.getKey());

            List<HbaseColumnValue> values = entry.getValue();

            for (HbaseColumnValue value : values) {
                HBaseColumn column = value.column();
                put.addColumn(column.familtyBytes(), column.qualifierBytes(), value.value());
            }

            table.put(put);
        }

        if (connection == null) {
            conn.close();
        }
    }

    /**
     * put record
     *
     * @param connection
     * @param tablename
     * @param rowKey
     * @param values
     * @throws IOException
     */
    public void put(Connection connection, String tablename, byte[] rowKey, List<HbaseColumnValue> values) throws IOException {
        Map<byte[], List<HbaseColumnValue>> records = new HashMap<byte[], List<HbaseColumnValue>>();
        records.put(rowKey, values);

        puts(connection, tablename, records);

        records.clear();
    }
}
