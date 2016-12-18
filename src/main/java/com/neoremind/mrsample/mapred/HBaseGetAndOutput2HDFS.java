package com.neoremind.mrsample.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Use IntelliJ IDEA to launch mr job
 * <p>
 * Run->Configuration->Application
 * <p>
 * Main class is com.neoremind.mrsample.mapred.HBaseGetAndOutput2HDFS
 * <p>
 * Program arguments is hdfs://localhost:9000/test
 *
 * @author zhangxu
 */
public class HBaseGetAndOutput2HDFS extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new HBaseGetAndOutput2HDFS(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //Configuration conf = new Configuration(); // When extends from Configured, do not use this
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(HBaseGetAndOutput2HDFS.class);
        Scan scan = new Scan();
        scan.setMaxVersions(); // get all versions
//        scan.setStartRow(Bytes.toBytes("3136947"));
//        scan.setStopRow(Bytes.toBytes("3136947" + 1));
//
//        scan.addColumn(Bytes.toBytes("info"),
//                Bytes.toBytes("name"));
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(
                "mytest",        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper
                Text.class,             // mapper output key
                Text.class,             // mapper output value
                job);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setNumReduceTasks(2); //set to 0 if no reducer needed
        job.waitForCompletion(true);
        return 0;
    }

    static class MyMapper extends TableMapper<Text, Text> {
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            for (Cell cell : value.rawCells()) {
                context.write(new Text(row.copyBytes()), new Text(cell.toString()));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
