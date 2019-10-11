package com.example.hadoop;

import com.example.hadoop.Mapreduce.Flow.FlowBeam;
import com.example.hadoop.Mapreduce.Flow.FlowMapper;
import com.example.hadoop.Mapreduce.Flow.FlowPartitioner;
import com.example.hadoop.Mapreduce.Flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//@SpringBootApplication
public class HadoopApplication {

    //	public static void main(String[] args) {
//		SpringApplication.run(HadoopApplication.class, args);
//	}
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("-------------------------------------------");
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","hdfs://192.168.6.61:9000/");
        Job job = Job.getInstance(conf);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //设置map task 数据分区，以及reduce task数量
        /*job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(6);*/

        //如果不设置inputformat，默认使用TextInputFormat.Class，默认就是有几个block切片就会有几个split切片，就会启动几个map task
        /*job.setInputFormatClass(CombineTextInputFormat.class);
        //设置切片大小范围
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1004);
        CombineTextInputFormat.setMinInputSplitSize(job, 2 * 1024 * 1004);*/

        //用于环形缓存数据溢出，把何用的key的value合并起来，避免数据分区的同一个key出现多次导致分区的数据文件过大
        /*job.setCombinerClass(FlowReducer.class);*/


//        job.setJar("/home/hadoop.jar");
        job.setJarByClass(HadoopApplication.class);

        job.setMapOutputKeyClass(FlowBeam.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeam.class);

        FileInputFormat.setInputPaths(job, new Path("/outputflow"));
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/sortflow"))) {
            fileSystem.delete(new Path("/sortflow"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/sortflow"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
