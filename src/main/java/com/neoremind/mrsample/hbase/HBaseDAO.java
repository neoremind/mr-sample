package com.neoremind.mrsample.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase Data Access Object
 */
public class HBaseDAO implements Closeable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Configuration configuration;
    private Connection connection;
    private Admin admin;

    public static HBaseDAO newInsance() {
        return new HBaseDAO();
    }

    private HBaseDAO() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.master", "hdfs://localhost:60000");
        configuration.set("hbase.root.dir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void createTable(String tableName, String[] cols) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            logger.warn(tableName + " exists.");
        } else {
            HTableDescriptor hTableDesc = new HTableDescriptor(tName);
            Arrays.stream(cols).forEach(col -> {
                HColumnDescriptor hColumnDesc = new HColumnDescriptor(col);
                hColumnDesc.setMaxVersions(100); // by default allow 100 versions
                hTableDesc.addFamily(hColumnDesc);
            });
            admin.createTable(hTableDesc);
        }
    }

    public void deleteTable(String tableName) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
        } else {
            logger.warn(tableName + " not exists.");
        }
    }

    public void listTables() throws IOException {
        Arrays.stream(admin.listTables()).forEach(t -> logger.info(t.toString()));
    }

    public void put(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);
        table.close(); //TODO finally block needed
    }

    public void put(String tableName, String rowKey, Cell kv) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(kv);
        table.put(put);
        table.close();
    }

    public void batchPut(String tableName, String rowKey, List<Cell> kvs) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = Lists.newArrayList();
        for (Cell cell : kvs) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(cell);
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }


    public Result get(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        if (colFamily != null && col != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        Result result = table.get(get);
        showCell(result);
        table.close();
        return result;
    }

    public Result getAllVersions(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.setMaxVersions();
        if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        if (colFamily != null && col != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        Result result = table.get(get);
        showCell(result);
        table.close();
        return result;
    }

    public Result get(String tableName, String rowKey) throws IOException {
        return get(tableName, rowKey, null, null);
    }

    public void delete(String tableName, String rowKey, String colFamily, String col) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            logger.error(tableName + " not exists.");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete del = new Delete(Bytes.toBytes(rowKey));
        if (colFamily != null && col == null) {
            del.addFamily(Bytes.toBytes(colFamily));
        }
        if (colFamily != null && col != null) {
            del.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        table.delete(del);
        table.close();
    }

    public void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            logger.info("RowKey={}, Column={}.{}, ts={}, Value={}", new String(CellUtil.cloneRow(cell)),
                    new String(CellUtil.cloneFamily(cell)),
                    new String(CellUtil.cloneQualifier(cell)),
                    cell.getTimestamp(),
                    new String(CellUtil.cloneValue(cell)));
        }
    }

}