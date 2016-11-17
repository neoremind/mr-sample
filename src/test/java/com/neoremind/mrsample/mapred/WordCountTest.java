package com.neoremind.mrsample.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * mrunit has been retired since Apr, 2016.
 * <p/>
 * https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial
 *
 * @author zhangxu
 */
public class WordCountTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordMatchMapper mapper = new WordMatchMapper();
        IntSumReducer reducer = new IntSumReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "word1 word2 word3 word1 word2"));
        mapDriver.withOutput(new Text("word1"), new IntWritable(1))
                .withOutput(new Text("word2"), new IntWritable(1))
                .withOutput(new Text("word3"), new IntWritable(1))
                .withOutput(new Text("word1"), new IntWritable(1))
                .withOutput(new Text("word2"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = Lists.newArrayList();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("word1"), values);
        reduceDriver.withInput(new Text("word2"), Lists.newArrayList(new IntWritable(3)));
        reduceDriver.withOutput(new Text("word1"), new IntWritable(2));
        reduceDriver.withOutput(new Text("word2"), new IntWritable(3));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(0), new Text("word1 word3 word2 word1 word2"));
        mapReduceDriver.withOutput(new Text("word1"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("word2"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("word3"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

}
