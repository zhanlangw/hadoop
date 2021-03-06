package com.example.hadoop.Mapreduce.Flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * map task 输出分区
 */
public class FlowPartitioner extends Partitioner<FlowBeam, Text> {
    private static Map<String, Integer> map = new HashMap();

    static {
        map.put("135", 0);
        map.put("136", 1);
        map.put("137", 2);
        map.put("138", 3);
        map.put("139", 4);
    }

    //默认是key的hashcode对reduce task数量取模得到分区号
    @Override
    public int getPartition(FlowBeam key, Text flowBeam, int reduceNum) {
        String str = flowBeam.toString().substring(0, 3);
        Integer num = map.get(str);
        return num == null ? 5 : num;
    }
}