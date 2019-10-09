package com.example.hadoop.Mapreduce.Flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * map task 输出分区
 */
public class FlowPartitioner extends Partitioner<Text, FlowBeam> {
    private static Map<String, Integer> map = new HashMap<>();

    static {
        map.put("135", 0);
        map.put("136", 1);
        map.put("137", 2);
        map.put("138", 3);
        map.put("139", 4);
    }

    @Override
    public int getPartition(Text key, FlowBeam flowBeam, int i) {
        String str = key.toString().substring(0, 3);
        Integer num = map.get(str);
        return num == null ? 5 : num;
    }
}
