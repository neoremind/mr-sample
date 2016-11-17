package com.neoremind.mrsample.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * 从distribute cache获取黑名单词的mapper
 *
 * @author zhangxu
 */
public class WordBlacklistMathMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Set<String> blacklistWords = Sets.newHashSet();

    private static Logger logger = LoggerFactory.getLogger(WordBlacklistMathMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(configuration);
        Path blacklistWordsFile = localCacheFiles[0];
        logger.info("Loading stop words from file: " + blacklistWordsFile.getName());
        FSDataInputStream inputStream = FileSystem.getLocal(configuration).open(blacklistWordsFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String word = bufferedReader.readLine();
        while (word != null) {
            blacklistWords.add(word);
            word = bufferedReader.readLine();
        }
        bufferedReader.close();
        logger.info(String.format("Loaded %d stop words from line %s", blacklistWords.size(),
                blacklistWordsFile.getName()));
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            if (!blacklistWords.contains(word.toLowerCase())) {
                context.write(new Text(word), new IntWritable(1));
            } else {
                context.getCounter(SKIPPED_WORDS.BLACKLIST_MATCH_WORD).increment(1);
            }
        }
    }

    enum SKIPPED_WORDS {
        BLACKLIST_MATCH_WORD
    }

}

