package com.example.hadoop.Mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        data = data.trim();
        data = data.replace(" ", "");
        for (int i = 0; i < data.length(); i++) {
            context.write(new Text((char)(data.indexOf(i)) + ""), new IntWritable(i));
        }
    }
}
