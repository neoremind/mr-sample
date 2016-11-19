package com.neoremind.mrsample.mapred.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 *     productId  productName productNumber
 *     1  Iphone 5
 *     2  Macbook 88
 *     3  NikeShoes 200
 *     4  Bag 500
 *     5  Car 400
 * </pre>
 *
 * @author zhangxu
 */
public class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().isEmpty()) {
            return;
        }
        String[] recordFields = value.toString().split("\\t");
        int productId = Integer.parseInt(recordFields[0]);
        String productName = recordFields[1];
        String productNumber = recordFields[2];

        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
        ProductRecord record = new ProductRecord(productName, productNumber);
        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
}
