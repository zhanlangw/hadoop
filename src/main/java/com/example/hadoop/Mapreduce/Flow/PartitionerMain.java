package com.example.hadoop.Mapreduce.Flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 数据分区
 */
public class PartitionerMain {
    static class PartitionerMapper extends Mapper<LongWritable, Text, FlowBeam, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            context.write(new FlowBeam(Long.parseLong(data[1]), Long.parseLong(data[2])), new Text(data[0]));
        }
    }

    static class PartitionerReducer extends Reducer<FlowBeam, Text, Text, FlowBeam> {
        @Override
        protected void reduce(FlowBeam key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.defaultFS", "hdfs://192.168.81.11:9000/");
        Job job = Job.getInstance(conf);

        //压缩配置
//        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
//        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);

        job.setJarByClass(PartitionerMain.class);

        job.setMapperClass(PartitionerMapper.class);
        job.setReducerClass(PartitionerReducer.class);

        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(6);

        job.setMapOutputKeyClass(FlowBeam.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeam.class);

//        //添加压缩配置
//        FileOutputFormat.setCompressOutput(job, true);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        TextInputFormat.setInputPaths(job, new Path("/telephone-result/part-r-00000.gz"));
        //指定处理完成之后的结果所保存的位置
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/telephone-partitoner"))) {
            fileSystem.delete(new Path("/telephone-partitoner"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/telephone-partitoner"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
