package com.example.hadoop.Mapreduce.Flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBeam, Text, Text, FlowBeam> {
    @Override
    protected void reduce(FlowBeam key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), key);
    }
}
