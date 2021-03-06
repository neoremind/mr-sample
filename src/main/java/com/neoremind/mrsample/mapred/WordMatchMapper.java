package com.neoremind.mrsample.mapred;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Match ascii character word only
 *
 * @author zhangxu
 */
public class WordMatchMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Pattern wordPattern = Pattern.compile("[^a-z0-9]*([a-z0-9]+)[^a-z0-9]*");

    @Override
    public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();
        String[] tokens = line.split(" ");
        for (String token : tokens) {
            Matcher wordMatcher = wordPattern.matcher(token.toLowerCase());
            if (wordMatcher.matches()) {
                String word = wordMatcher.group(1);
                context.write(new Text(word), new IntWritable(1));
            } else {
                context.getCounter(SKIPPED_WORDS.MISMATCHED_WORD).increment(1);
            }
        }
    }

    enum SKIPPED_WORDS {
        MISMATCHED_WORD
    }

}
