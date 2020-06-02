package com.example.hadoop.Mapreduce.Flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBeam, Text > {

    private FlowBeam beam = new FlowBeam();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        Text telephone = new Text(split[0]);

        beam.setUpflow(Long.parseLong(split[split.length-3]));
        beam.setDownflow(Long.parseLong(split[split.length-2]));
        beam.setSumFlow(beam.getDownflow()+beam.getUpflow());

        context.write(beam, telephone);
    }
}
