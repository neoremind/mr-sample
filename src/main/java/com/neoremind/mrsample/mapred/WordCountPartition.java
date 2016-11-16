package com.neoremind.mrsample.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Word count with {@link LexicalPartitioner} and facilitate {@link ToolRunner} to execute job
 * <p/>
 * Example link: https://github.com/yhemanth/hadoop-training-samples
 *
 * <pre>
 *     bin/hadoop jar share/hadoop/mapreduce/mr-sample.jar com.neoremind.mrsample.mapred.WordCountPartition  /fewwords /output4
 * </pre>
 *
 * @author zhangxu
 */
public class WordCountPartition extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WordCountPartition(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job wordCountJob = Job.getInstance(conf, "my word count with partition");
        wordCountJob.setJarByClass(WordCountPartition.class);
        wordCountJob.setMapperClass(WordMatchMapper.class);
        if (conf.getBoolean("wordcount.runcombiner", false)) {
            wordCountJob.setCombinerClass(IntSumReducer.class);
        }
        if (conf.getBoolean("wordcount.partitioner.lexical", true)) {
            wordCountJob.setPartitionerClass(LexicalPartitioner.class);
        }
        wordCountJob.setReducerClass(IntSumReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
        wordCountJob.setNumReduceTasks(4);
        wordCountJob.waitForCompletion(true);
        return 0;
    }

}
