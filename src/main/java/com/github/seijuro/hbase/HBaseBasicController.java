package com.github.seijuro.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Created by seijuro
 */
public class HBaseBasicController {
    /**
     * Class Properties.
     */
    private final Logger LOG = LoggerFactory.getLogger(HBaseBasicController.class);

    private static final int DefaultMaxVersion = 1;

    /**
     * Instance Properties
     */
    private Connection conn;
    private Configuration config;
    private User user;

    /**
     * C'tor
     *
     * @param $config
     */
    public HBaseBasicController(Configuration $config, User $user) throws IOException {
        this.config = $config;
        this.user = $user;

        this.conn = ConnectionFactory.createConnection(this.config);
    }

    /**
     * C'tor
     *
     * @param $config
     * @throws IOException
     */
    public HBaseBasicController(Configuration $config) throws IOException {
        this($config, null);
    }

    /**
     * C'tor
     *
     * @param $config
     * @param $user
     * @throws IOException
     */
    public HBaseBasicController(HBaseConfig $config, User $user) throws IOException {
        this($config.create(), $user);
    }

    /**
     * C'tor
     *
     * @param $config
     * @throws IOException
     */
    public HBaseBasicController(HBaseConfig $config) throws IOException {
        this($config, null);
    }

    /**
     * get hbase connection.
     * If there weren't an existing connection, this will create and return new connection.
     * Otherwise, it will return exisinng connection.
     *
     * @return
     * @throws IOException
     */
    protected Connection getConnection() throws IOException {
        if (Objects.isNull(this.conn) ||
                this.conn.isClosed()) {
            this.conn = ConnectionFactory.createConnection(this.config);
        }

        return this.conn;
    }

    /**
     * get Table instance.
     *
     * @param conn
     * @param tablename
     * @return
     * @throws IOException
     */
    public Table getTable(Connection conn, String tablename) throws IOException {
        assert Objects.nonNull(conn) && !conn.isClosed();
        assert StringUtils.isNotEmpty(tablename);

        return conn.getTable(TableName.valueOf(tablename));
    }

    /**
     * get Table instance.
     *
     * @param tablename
     * @return
     * @throws IOException
     */
    public Table getTable(String tablename) throws IOException {
        return getTable(getConnection(), tablename);
    }

    /**
     * return Admin instance.
     *
     * @return
     * @throws IOException
     */
    public Admin getAdmin() throws IOException {
        Connection conn = getConnection();

        assert Objects.nonNull(conn) && !conn.isClosed();

        return conn.getAdmin();
    }

    /**
     * check if table whose name is tablename exists.
     * return true if exists. Otherwise, return false.
     *
     * @param admin
     * @param tablename
     * @return
     * @throws IOException
     */
    public boolean existsTable(Admin admin, String tablename) throws IOException {
        //  assert(s)
        assert Objects.nonNull(admin);
        assert StringUtils.isNotEmpty(tablename);

        return admin.tableExists(TableName.valueOf(tablename));
    }

    /**
     * check if table whose name is tablename exists.
     * return true if exists. Otherwise, return false.
     *
     * @param tablename
     * @return
     * @throws IOException
     */
    public boolean existsTable(String tablename) throws IOException {
        Admin admin = getAdmin();

        //  assert
        assert Objects.nonNull(admin);

        try {
            return existsTable(admin, tablename);
        }
        finally {
            admin.close();
        }
    }

    /**
     * create table with given tablename and column family(s)
     *
     * @param admin
     * @param tablename
     * @param families
     * @throws IOException
     */
    public void createTable(Admin admin, String tablename, String[] families) throws IOException {
        //  assert(s)
        assert Objects.nonNull(admin);
        assert StringUtils.isNotEmpty(tablename);
        assert families.length > 0;

        HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(tablename));
        for (String family : families) {
            htable.addFamily(new HColumnDescriptor(family));
        }

        admin.createTable(htable);
    }

    /**
     * create table.
     *
     * @param tablename
     * @param families
     * @throws IOException
     */
    public void createTable(String tablename, String[] families) throws IOException {
        Admin admin = getAdmin();

        try {
            createTable(admin, tablename, families);
        }
        finally {
            admin.close();
        }
    }

    /**
     * disable & drop table whose name is tablename.
     *
     * @param admin
     * @param tablename
     * @throws IOException
     */
    public void dropTable(Admin admin, String tablename) throws IOException {
        //  assert(s)
        assert Objects.nonNull(admin);
        assert StringUtils.isNotEmpty(tablename);

        if (!admin.isTableDisabled(TableName.valueOf(tablename))) {
            admin.disableTable(TableName.valueOf(tablename));
        }

        admin.deleteTable(TableName.valueOf(tablename));
    }

    /**
     * disable & drop table whose name is tablename.
     *
     * @param tablename
     * @throws IOException
     */
    public void dropTable(String tablename) throws IOException {
        Admin admin = getAdmin();

        try {
            dropTable(admin, tablename);
        }
        finally {
            admin.close();
        }
    }

    /**
     * close connection if connnected.
     *
     * @throws IOException
     */
    protected void close() throws IOException {
        if (Objects.nonNull(this.config) &&
                !this.conn.isClosed()) {
            this.conn.close();
        }

        this.conn = null;
    }

    /**
     * put value into single column.
     *
     * @param table
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public void put(Table table, byte[] rowKey, byte[] family, byte[] column, byte[] value) throws IOException, IllegalArgumentException {
        //  check param #1 - table
        if (Objects.isNull(table)) {
            String msg = "Parameter, table, is null object.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        //  check param #2 - rowKey
        if (Objects.isNull(rowKey) ||
                rowKey.length == 0) {
            String msg = "Parameter, rowKey, is empy.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        if (Objects.nonNull(family) &&
                Objects.nonNull(column) &&
                Objects.nonNull(value)) {
            Put put = new Put(rowKey);

            put.addColumn(family, column, value);
            table.put(put);
        }
    }

    /**
     * delete row whose key is rowKey.
     *
     * @param table
     * @param rowkey
     * @throws IOException
     */
    public void delete(Table table, byte[] rowkey) throws IOException {
        assert Objects.nonNull(table);

        if (Objects.nonNull(rowkey)) {
            Delete delete = new Delete(rowkey);

            table.delete(delete);
        }
    }

    /**
     * delete row whose key is rowKey.
     *
     * @param tablename
     * @param rowkey
     * @throws IOException
     */
    public void delete(String tablename, byte[] rowkey) throws IOException {
        Table table = getTable(tablename);

        try {
            delete(table, rowkey);
        }
        finally {
            table.close();
        }
    }

    /**
     *
     *
     * @param table
     * @param rowKey
     * @param families
     * @param columns
     * @param values
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public void put(Table table, byte[] rowKey, byte[][] families, byte[][] columns, byte[][] values) throws IOException, IllegalArgumentException {
        //  check param #1 - table
        if (Objects.isNull(table)) {
            String msg = "Parameter, table, is null object.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        //  check param #2 - rowKey
        if (Objects.isNull(rowKey) ||
                rowKey.length == 0) {
            String msg = "Parameter, rowKey, is empy.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        if (Objects.isNull(families) ||
                Objects.isNull(columns) ||
                Objects.isNull(values)) {
            String msg = "At least one of parameters(families, columns, values) is null object.";

            //  Log (WARN)
            LOG.warn(msg);

            return;
        }

        if (families.length != columns.length ||
                families.length != values.length) {
            String msg = "3 Params must have same length (families, columns, values).";

            //  Log (WARN)
            LOG.warn(msg);

            return;
        }

        Put put = new Put(rowKey);

        for (int index = 0; index != families.length; ++index) {
            put.addColumn(families[index], columns[index], values[index]);
        }

        table.put(put);
    }

    /**
     * put row
     *
     * @param table
     * @param rowKey
     * @param columnValues
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public void put(Table table, byte[] rowKey, List<HbaseColumnValue> columnValues) throws IOException, IllegalArgumentException {
        //  check param #1 - table
        if (Objects.isNull(table)) {
            String msg = "Parameter, table, is null object.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        //  check param #2 - rowKey
        if (Objects.isNull(rowKey) ||
                rowKey.length == 0) {
            String msg = "Parameter, rowKey, is empy.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        if (Objects.isNull(columnValues) ||
                columnValues.size() == 0) {
            LOG.warn("Param, columnValues, is empty.");

            return;
        }

        Put put = new Put(rowKey);

        for (HbaseColumnValue columnValue : columnValues) {
            HBaseColumn column = columnValue.getColumn();
            byte[] value = columnValue.getValue();

            put.addColumn(column.toBytesFamily(), column.toBytesQualifier(), value);
        }

        table.put(put);
    }

    /**
     * retrieve row whose key is {@param rowKey}
     *
     * @param table
     * @param rowKey
     * @param maxVersion
     * @return
     * @throws IOException
     * @throws IllegalArgumentException
     */
    protected Result get(Table table, byte[] rowKey, int maxVersion) throws IOException, IllegalArgumentException {
        if (Objects.isNull(table)) {
            String msg = "Param, table, is null object.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        if (Objects.isNull(rowKey) ||
                rowKey.length == 0) {
            String msg = "Param, rowKey, is empty.";

            //  Log (WARN)
            LOG.warn(msg);

            throw new IllegalArgumentIOException(msg);
        }

        Get get = new Get(rowKey);
        get.setMaxVersions(maxVersion);

        Result result = table.get(get);

        LOG.debug("result (rowkey : {}) : {}", rowKey, result);

        return result;
    }

    /**
     * retrieve row whose key is {@param rowKey}
     *
     * @param table
     * @param rowKey
     * @return
     * @throws IOException
     */
    public Result get(Table table, byte[] rowKey) throws IOException {
        return get(table, rowKey, DefaultMaxVersion);
    }

    /**
     * create {@link Scan} instance.
     * You can get {@link Scan} instance to retrieve rows which ragge between {@param startRow} and {@param stopRow} by specifing these parameter(s).
     * If you didn't set, return {@link Scan} instance to retrieve all rows.
     *
     * @param startRow
     * @param stopRow
     * @param familyOrColumns
     * @param filter
     * @return
     */
    public <T extends HBaseColumnFamily> Scan createScan(byte[] startRow, byte[] stopRow, List<T> familyOrColumns, Filter filter) {
        Scan scan;

        if (Objects.isNull(startRow) ||
                startRow.length == 0) {
            scan = new Scan();
        }
        else if (Objects.isNull(stopRow) ||
                stopRow.length == 0) {
            scan = new Scan(startRow);
        }
        else {
            scan = new Scan(startRow, stopRow);
        }

        for (T familyOrColumn : familyOrColumns) {
            if (familyOrColumn instanceof HBaseColumn) {
                HBaseColumn column = HBaseColumn.class.cast(familyOrColumn);

                scan.addColumn(column.toBytesFamily(), column.toBytesQualifier());
            }
            else {
                HBaseColumnFamily family = HBaseColumnFamily.class.cast(familyOrColumn);

                scan.addFamily(family.toBytesFamily());
            }
        }

        if (Objects.nonNull(filter)) {  scan.setFilter(filter); }

        return scan;
    }

    /**
     * scan row whose rowkey range between {@param startRow} and {@param stopRow} from {@param table}
     * if set {@param startRow} or {@param stopRow} or both set null, the parameter set null would be ignored.
     *
     * @param table
     * @param startRow
     * @param stopRow
     * @param familyOrColumns
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T extends HBaseColumnFamily> List<Result> scan(Table table, byte[] startRow, byte[] stopRow, List<T> familyOrColumns, Filter filter) throws IOException {
        ResultScanner scanner = table.getScanner(createScan(startRow, stopRow, familyOrColumns, filter));
        List<Result> results = new ArrayList<>();

        do {
            Result result = scanner.next();

            if (Objects.isNull(result)) {
                break;
            }

            results.add(result);
        } while (true);

        scanner.close();

        return results;
    }

    /**
     * scan {@param table} from the row whose rowkey is {@param startRow}.
     *
     * @param table
     * @param startRow
     * @param familyOrColumns
     * @param filter
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T extends HBaseColumnFamily> List<Result> scan(Table table, byte[] startRow, List<T> familyOrColumns, Filter filter) throws IOException {
        ResultScanner scanner = table.getScanner(createScan(startRow, null, familyOrColumns, filter));
        List<Result> results = new ArrayList<>();

        do {
            Result result = scanner.next();

            if (Objects.isNull(result)) {
                break;
            }

            results.add(result);
        } while (true);

        scanner.close();

        return results;
    }

    /**
     * retrieve all rows.
     * You can easily get {@param table first parameter} instance by calling {@link #getTable(String)} interface.
     * if set {@param filter} null, it would be ignored.
     *
     * @param table
     * @param familyOrColumns
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T extends HBaseColumnFamily> List<Result> scan(Table table, List<T> familyOrColumns, Filter filter) throws IOException {
        ResultScanner scanner = table.getScanner(createScan(null, null, familyOrColumns, filter));
        List<Result> results = new ArrayList<>();

        do {
            Result result = scanner.next();

            if (Objects.isNull(result)) {
                break;
            }

            results.add(result);
        } while (true);

        scanner.close();

        return results;
    }
 }
