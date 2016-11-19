package com.neoremind.mrsample.mapred.join;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangxu
 */
public class JoinRecuder extends Reducer<ProductIdKey, JoinGenericWritable, NullWritable, Text> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public void reduce(ProductIdKey key, Iterable<JoinGenericWritable> values, Context context) throws
            IOException, InterruptedException {
        StringBuilder output = new StringBuilder();
        int sumOrderQty = 0;
        double sumLineTotal = 0.0;

        for (JoinGenericWritable v : values) {
            Writable record = v.get();
            if (key.recordType.equals(ProductIdKey.PRODUCT_RECORD)) {
                ProductRecord pRecord = (ProductRecord) record;
                output.append(Integer.parseInt(key.productId.toString())).append(", ");
                output.append(pRecord.productName.toString()).append(", ");
                output.append(pRecord.productNumber.toString()).append(", ");
            } else {
                SalesOrderDataRecord record2 = (SalesOrderDataRecord) record;
                sumOrderQty += Integer.parseInt(record2.orderQty.toString());
                sumLineTotal += Double.parseDouble(record2.lineTotal.toString());
            }
        }

        logger.info(output.toString() + sumOrderQty + ", " + sumLineTotal);
        //        if (sumOrderQty > 0) {
        //            context.write(NullWritable.get(), new Text(output.toString() + sumOrderQty + ", " +
        // sumLineTotal));
        //        }
        context.write(NullWritable.get(), new Text(output.toString() + "," + sumLineTotal));
    }
}
