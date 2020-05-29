package com.example.hadoop.Mapreduce.WordCount;

import com.example.hadoop.Mapreduce.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","hdfs://192.168.81.11:9000/");
        Job job = Job.getInstance(conf);

//        job.setJar("/hadoop.jar");
        job.setJarByClass(WordCountRunner.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(job, new Path("/profile"));
        //指定处理完成之后的结果所保存的位置
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/sortflow"))) {
            fileSystem.delete(new Path("/sortflow"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/sortflow"));

        //向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);


    }
}
