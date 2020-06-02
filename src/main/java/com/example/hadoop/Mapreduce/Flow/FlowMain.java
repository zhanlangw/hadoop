package com.example.hadoop.Mapreduce.Flow;

import com.example.hadoop.Mapreduce.friend.Friend2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "hdfs://192.168.81.11:9000/");
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowMain.class);

        job.setMapperClass(FlowMapper.class);
//        job.setPartitionerClass(FlowPartitioner.class);
        job.setReducerClass(FlowReducer.class);

//        job.setNumReduceTasks(6);

        job.setMapOutputKeyClass(FlowBeam.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(FlowBeam.class);

        FileInputFormat.setInputPaths(job, new Path("/telephone"));
        //指定处理完成之后的结果所保存的位置
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/telephone-result"))) {
            fileSystem.delete(new Path("/telephone-result"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/telephone-result"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
