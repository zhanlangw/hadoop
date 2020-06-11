package com.example.hadoop.Mapreduce.order;

import com.example.hadoop.Mapreduce.friend.Friend2;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;

import java.io.IOException;
import java.util.*;

public class OrderMain {
    static class OrderMapper extends Mapper<LongWritable, Text, Text, DomainBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] fields = data.split("\t");
            DomainBean bean;
            Text productId = new Text();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            if (fileName.startsWith("order")) {
                bean = new DomainBean(fields[0], fields[1], fields[3], fields[2], "", "");
                productId.set(fields[2]);
            } else {
                bean = new DomainBean("", "", "", fields[0], fields[1], fields[2]);
                productId.set(fields[0]);
            }
            context.write(productId, bean);
        }
    }

    static class OrderReducer extends Reducer<Text, DomainBean, Text, DomainBean> {
        @Override
        protected void reduce(Text key, Iterable<DomainBean> values, Context context) throws IOException, InterruptedException {
            Set<DomainBean> list = new HashSet<DomainBean>();
            Iterator<DomainBean> iterator = values.iterator();
            DomainBean domainBean = null;
            while (iterator.hasNext()) {
                DomainBean next = iterator.next();
                if (StringUtils.isNotEmpty(next.getProductName())) {
                    domainBean = next;
                } else {
                    list.add(next);
                }
            }

            assert domainBean != null;
            for (DomainBean bean : list) {
                bean.setProductName(domainBean.getProductName());
                bean.setProductDesc(domainBean.getProductDesc());
                context.write(new Text(bean.getPrice()), bean);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "root");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "hdfs://192.168.81.11:9000/");
        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderMain.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DomainBean.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(DomainBean.class);

        FileInputFormat.setInputPaths(job, new Path("/order"), new Path("/product"));
        //指定处理完成之后的结果所保存的位置
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/order-result"))) {
            fileSystem.delete(new Path("/order-result"), true);
        }
        FileOutputFormat.setOutputPath(job, new Path("/order-result"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
