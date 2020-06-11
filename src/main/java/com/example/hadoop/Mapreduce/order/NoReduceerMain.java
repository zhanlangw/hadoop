package com.example.hadoop.Mapreduce.order;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class NoReduceerMain {

    static class OrderMapper extends Mapper<LongWritable, Text, DomainBean, NullWritable> {

        Map<String, String> map = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            String path = cacheFiles[0].getPath();
            BufferedReader bufferedReader = new BufferedReader(new FileReader("./"+path));;
            String line = null;
            while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
                String[] split = line.split("\t");
                map.put(split[0], split[1] + "\t" + split[2]);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (map.containsKey(split[2])) {
                DomainBean bean = new DomainBean(split[0], split[1], split[3], split[2], map.get(split[2]).split("\t")[0], map.get(split[2]).split("\t")[1]);
                context.write(bean, NullWritable.get());
            } else {
                log.info("not found product id {}", value.toString());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");


        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master.hadoop");
        conf.set("fs.defaultFS", "hdfs://192.168.81.11:9000/");
        Job job = Job.getInstance(conf);

        job.setJarByClass(NoReduceerMain.class);
//        job.setJar("E:\\develop\\CODE\\hadoop\\out\\artifacts\\hadoop_jar\\hadoop.jar");
        job.setJar("/root/hadoop.jar");
        job.setNumReduceTasks(0);

        job.setMapperClass(OrderMapper.class);

        job.setOutputKeyClass(DomainBean.class);
        job.setOutputValueClass(NullWritable.class);


        // 指定需要缓存一个文件到所有的maptask运行节点工作目录
        /* job.addArchiveToClassPath(archive); */// 缓存jar包到task运行节点的classpath中
        /* job.addFileToClassPath(file); */// 缓存普通文件到task运行节点的classpath中
        /* job.addCacheArchive(uri); */// 缓存压缩包文件到task运行节点的工作目录
        /* job.addCacheFile(uri) */// 缓存普通文件到task运行节点的工作目录

        // 将产品表文件缓存到task工作节点的工作目录中去
//        job.addCacheFile(new URI("file:/D:/srcdata/mapjoincache/pdts.txt"));
        job.addCacheFile(new URI("hdfs://192.168.81.11:9000/product"));
//        job.addFileToClassPath(new Path("hdfs://192.168.81.11:9000/product"));
//        job.addArchiveToClassPath(new Path("hdfs://192.168.81.11:9000/product"));

        FileInputFormat.setInputPaths(job, new Path("/order"));
        //指定处理完成之后的结果所保存的位置
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/order-order"))) {
            fileSystem.delete(new Path("/order-order"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/order-order"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
