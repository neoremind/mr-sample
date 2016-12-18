package com.neoremind.mrsample.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Command: https://learnhbase.wordpress.com/2013/03/02/hbase-shell-commands/<br/>
 * <pre>
 * hbase(main):019:0> describe 'mytest'
 * Table mytest is ENABLED
 * mytest
 * COLUMN FAMILIES DESCRIPTION
 * {NAME => 'cf1', BLOOMFILTER => 'ROW', VERSIONS => '100', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'F
 * ALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLO
 * CKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
 * {NAME => 'cf2', BLOOMFILTER => 'ROW', VERSIONS => '100', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'F
 * ALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLO
 * CKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
 * 2 row(s) in 3.3720 seconds
 * </pre>
 * <p>
 * <pre>
 * hbase(main):031:0> get 'mytest','r1', {COLUMN=>'cf1', VERSIONS=>10}
 * COLUMN                     CELL
 * cf1:q1                    timestamp=1481960868812, value=v_r1
 * cf1:q1                    timestamp=1481960859052, value=v_r1
 * 2 row(s) in 0.5120 seconds
 * </pre>
 * <p>
 * <pre>
 * hbase(main):030:0> scan 'mytest', {VERSIONS=>3}
 * ROW                        COLUMN+CELL
 * r1                        column=cf1:q1, timestamp=1481960868812, value=v_r1
 * r1                        column=cf1:q1, timestamp=1481960859052, value=v_r1
 * 1 row(s) in 1.4160 seconds
 * </pre>
 * <p>
 * <pre>
 * hbase(main):008:0> scan 'mytest', {COLUMNS => ['cf1', 'cf2'], VERSIONS=> 100}
 * ROW                        COLUMN+CELL
 * r1                        column=cf1:q1, timestamp=1481989155491, value=v_r1
 * r1                        column=cf1:q1, timestamp=1481989155291, value=v_r1
 * r1                        column=cf1:q1, timestamp=1481961476924, value=v_r1
 * r1                        column=cf1:q1, timestamp=1481961440306, value=v_r1
 * r1                        column=cf1:q1, timestamp=1481961438033, value=v_r1
 * r1                        column=cf1:q1, timestamp=1481960868812, value=v_r1
 * r1                        column=cf1:q1, timestamp=1481960859052, value=v_r1
 * r1                        column=cf1:q1, timestamp=100, value=v_r1_1
 * r1                        column=cf1:q2, timestamp=104, value=v_r2_5
 * r1                        column=cf1:q2, timestamp=103, value=v_r2_4
 * r1                        column=cf1:q2, timestamp=102, value=v_r2_3
 * r1                        column=cf1:q2, timestamp=101, value=v_r2_2
 * r1                        column=cf1:q2, timestamp=100, value=v_r2_1
 * 1 row(s) in 0.0180 seconds
 * </pre>
 */
public class HBaseDAOTest {

    private static HBaseDAO hBaseDAO;

    public static final String TABLE = "mytest";
    public static final String CF1 = "cf1";
    public static final String CF2 = "cf2";

    @BeforeClass
    public static void setUp() throws IOException {
        hBaseDAO = HBaseDAO.newInsance();
        hBaseDAO.createTable(TABLE, new String[]{CF1, CF2});
    }

    @AfterClass
    public static void clear() throws IOException {
        //hBaseDAO.deleteTable("mytest");
        hBaseDAO.close();
    }

    @Test
    public void testListTables() throws IOException {
        hBaseDAO.listTables();
    }

    @Test(expected = NoSuchColumnFamilyException.class)
    public void testPutNegative() throws IOException {
        hBaseDAO.get(TABLE, "r1", "NO_EXIST", "q1");
    }

    @Test
    public void testPutAndGet() throws IOException {
        hBaseDAO.put(TABLE, "r1", "cf1", "q1", "v_testPutAndGet");
        Result result = hBaseDAO.get(TABLE, "r1", "cf1", "q1");
        assertThat(result.getRow(), is(Bytes.toBytes("r1")));
        assertThat(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q1")), is(Bytes.toBytes("v_testPutAndGet")));
        assertThat(result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("q1")).size(), is(1));
    }

    @Test
    public void testPutAndGet2() throws IOException {
        hBaseDAO.put(TABLE, "r1", new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("cf1"), Bytes.toBytes("q2"), Bytes.toBytes("v_testPutAndGet2")));
        Result result = hBaseDAO.get(TABLE, "r1", "cf1", "q2");
        assertThat(result.getRow(), is(Bytes.toBytes("r1")));
        assertThat(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q2")), is(Bytes.toBytes("v_testPutAndGet2")));
    }

    @Test
    public void testBatchPut() throws IOException {
        List<Cell> cellList = Lists.newArrayList(
                new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("cf1"), Bytes.toBytes("q1"), 100000l, Bytes.toBytes("v_testBatchPut")),
                new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("cf1"), Bytes.toBytes("q3"), 1004l, Bytes.toBytes("v_testBatchPut_1")),  //TODO overriding column so test cases is useless
                new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("cf1"), Bytes.toBytes("q3"), 1003l, Bytes.toBytes("v_testBatchPut_2")),
                new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("cf1"), Bytes.toBytes("q3"), 1002l, Bytes.toBytes("v_testBatchPut_3")),
                new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("cf1"), Bytes.toBytes("q3"), 1001l, Bytes.toBytes("v_testBatchPut_4")),
                new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("cf1"), Bytes.toBytes("q3"), 1000l, Bytes.toBytes("v_testBatchPut_5"))
        );
        hBaseDAO.batchPut(TABLE, "r1", cellList);
        Result result = hBaseDAO.getAllVersions(TABLE, "r1", "cf1", "q3");
        assertThat(result.getRow(), is(Bytes.toBytes("r1")));
        assertThat(result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("q3")).size(), greaterThan(4));
        assertThat(result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("q3")).get(0).getTimestamp(), is(1004l));
    }

    @Test
    public void testPutAndGetAllVersions() throws IOException {
        hBaseDAO.put(TABLE, "r1", "cf1", "q1", "v_testPutAndGetAllVersions_1");
        hBaseDAO.put(TABLE, "r1", "cf1", "q1", "v_testPutAndGetAllVersions_2");
        Result result = hBaseDAO.getAllVersions(TABLE, "r1", "cf1", "q1");
        assertThat(result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("q1")).size(), greaterThan(1));
    }

    @Test
    public void testDelete() throws IOException {
        hBaseDAO.put(TABLE, "r1", "cf1", "q99", "v_r1_99");
        Result result = hBaseDAO.get(TABLE, "r1", "cf1", "q99");
        assertThat(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q99")), is(Bytes.toBytes("v_r1_99")));
        hBaseDAO.delete(TABLE, "r1", "cf1", "q99");
        result = hBaseDAO.get(TABLE, "r1", "cf1", "q99");
        assertThat(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q99")), nullValue());
    }

    @Test
    public void testScan() throws IOException {
        hBaseDAO.scan(TABLE, "cf1", "q1");
    }

}
