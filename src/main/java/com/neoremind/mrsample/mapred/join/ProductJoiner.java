package com.neoremind.mrsample.mapred.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * Example link: http://www.codeproject.com/Articles/869383/Implementing-Join-in-Hadoop-Map-Reduce
 * <pre>
 *     bin/hadoop jar share/hadoop/mapreduce/mr-sample.jar com.neoremind.mrsample.mapred.join.ProductJoiner /product/sales_order.txt /product/product.txt /product/output7
 * </pre>
 *
 * <pre>
 *     bin/hdfs dfs -cat /product/output7/part-r-00000
 * </pre>
 *
 * Output will be:
 * <pre>
 *     1, Iphone, 5, ,500.56
 2, Macbook, 88, ,2250.0
 3, NikeShoes, 200, ,500.0
 4, Bag, 500, ,200.0
 5, Car, 400, ,550.0
 ,900.0
 * </pre>
 * @author zhangxu
 */
public class ProductJoiner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ProductJoiner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Join test");

        job.setJarByClass(ProductJoiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(ProductIdKey.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesOrderDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductMapper.class);

        job.setReducerClass(JoinRecuder.class);

        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean status = job.waitForCompletion(true);
        if (status) {
            return 0;
        } else {
            return 1;
        }
    }
}
