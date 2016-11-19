package com.neoremind.mrsample.mapred.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 *     orderId  productId quantity lineTotal
 *     1  1  100  100.56
 *     2  2  200  300.00
 *     3  1  100  400.00
 *     3  3  100  500.00
 *     3  4  100  200.00
 *     3  5  100  500.00
 *     3  2  100  1600.00
 *     3  5  100  50.00
 *     3  2  100  350.00
 *     3  99  100  900.00
 * </pre>
 *
 * @author zhangxu
 */
public class SalesOrderDataMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (value.toString().isEmpty()) {
            return;
        }
        String[] recordFields = value.toString().split("\\t");
        int productId = Integer.parseInt(recordFields[1]);
        int orderQty = Integer.parseInt(recordFields[2]);
        double lineTotal = Double.parseDouble(recordFields[3]);

        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.DATA_RECORD);
        SalesOrderDataRecord record = new SalesOrderDataRecord(orderQty, lineTotal);

        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
}

