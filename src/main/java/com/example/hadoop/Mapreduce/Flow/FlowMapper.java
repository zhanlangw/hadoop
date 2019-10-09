package com.example.hadoop.Mapreduce.Flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBeam> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        FlowBeam flowBeam = new FlowBeam(Long.parseLong(split[split.length - 2]), Long.parseLong(split[split.length - 3]));
        context.write(new Text(split[1]), flowBeam);
    }
}
