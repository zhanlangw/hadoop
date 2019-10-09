package com.example.hadoop.Mapreduce.Flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBeam, Text, FlowBeam> {
    @Override
    protected void reduce(Text key, Iterable<FlowBeam> values, Context context) throws IOException, InterruptedException {
        long upflow = 0;
        long downflow = 0;
        for (FlowBeam flow : values) {
            upflow += flow.getUpflow();
            downflow += flow.getDownflow();
        }
        FlowBeam flowBeam = new FlowBeam(upflow, downflow);
        context.write(key, flowBeam);
    }
}
