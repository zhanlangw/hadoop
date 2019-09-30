package com.example.hadoop.Mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicLong count = new AtomicLong();
        values.forEach(num -> count.addAndGet(num.get()));
        context.write(key, new LongWritable(count.get()));
    }
}
