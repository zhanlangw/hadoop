package com.example.hadoop;

import com.example.hadoop.Mapreduce.WordCountMapper;
import com.example.hadoop.Mapreduce.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class HadoopApplication {

    //	public static void main(String[] args) {
//		SpringApplication.run(HadoopApplication.class, args);
//	}
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("-------------------------------------------");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setJar("/home/hadoop.jar");
        job.setJarByClass(HadoopApplication.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, "hdfs://zhanlang1:9000/file");
        FileOutputFormat.setOutputPath(job, new Path("hdfs://zhanlang1:9000/outputfile"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }

}
