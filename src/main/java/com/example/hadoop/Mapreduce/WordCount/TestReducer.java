package com.example.hadoop.Mapreduce.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestReducer extends Reducer<Text, LongWritable,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {
        int count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        IntWritable intWritable = new IntWritable(count);

        context.write(key,intWritable);
    }
}
