package com.example.hadoop.Mapreduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;

public class TestRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");


        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master.hadoop");
        conf.set("fs.defaultFS", "hdfs://192.168.81.11:9000/");
        Job job = Job.getInstance(conf, "word count");

//        job.setJarByClass(TestRunner.class);
//        job.setJar("E:\\develop\\CODE\\hadoop\\target\\hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar");
        job.setJar("/root/hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar");

        job.setMapperClass(TestMapper.class);
//        job.setCombinerClass(TestReducer.class);
        job.setReducerClass(TestReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果不设置InputFormat，它默认用的是TextInputformat.class ,可以讲小文件合成大文件提交给maptask
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

        FileInputFormat.setInputPaths(job, new Path("/profile"));
        //指定处理完成之后的结果所保存的位置
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/sortflow"))) {
            fileSystem.delete(new Path("/sortflow"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/sortflow"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
