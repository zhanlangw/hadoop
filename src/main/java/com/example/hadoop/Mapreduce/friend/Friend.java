package com.example.hadoop.Mapreduce.friend;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * 找共同的好友
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 * D:A,E,F,L
 * E:B,C,D,M,L
 * F:A,B,C,D,E,O,M
 * G:A,C,D,E,F
 * H:A,C,D,E,O
 * I:A,O
 * J:B,O
 * K:A,C,D
 * L:D,E,F
 * M:E,F,G
 * O:A,H,I,J
 * 求出两两之间有共同好友的"用户对"，及他俩的共同好友
 * 比如:
 * a-b :  c ,e
 */
@Slf4j
public class Friend {
    static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString().trim();
            final String[] split = data.split(":");
            k.set(split[0]);
            for (String string : split[1].split(",")) {
                v.set(string);
                context.write(v, k);
            }
        }
    }

    static class FriendReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            StringBuffer sb = new StringBuffer();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                sb.append(iterator.next());
                sb.append(" ");
            }
            String value = sb.toString().trim();
            context.write(key, new Text(value));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "hdfs://192.168.81.11:9000/");
        Job job = Job.getInstance(conf);

        job.setJarByClass(Friend.class);

        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/test"));
        //指定处理完成之后的结果所保存的位置
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/friend"))) {
            fileSystem.delete(new Path("/friend"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/friend"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
