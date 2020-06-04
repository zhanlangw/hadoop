package com.example.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

public class HadoopClient {
    private FileSystem fs;
    private Configuration conf;

    //初始化
    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        conf = new Configuration();
        //可配置参数,
//        conf.set("dfs.replication", "2");
//        conf.set("fs,defaultFS","hdfs://192.168.6.61:9000");

        //得到一个客户端对象
        fs = FileSystem.get(new URI("hdfs://192.168.81.11:9000"), conf, "root");
    }

    //上传文件
    @Test
    public void upload() throws IOException {
//        fs.copyFromLocalFile(new Path("E:\\aaa.txt"), new Path("/ccc.txt"));
        //下载
        fs.copyToLocalFile(new Path("/aaa.txt"), new Path("E:\\bb.txt"));
        fs.close();
    }

    //查看参数
    @Test
    public void testConfig() {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            System.out.println(next.getKey() + "-----" + next.getValue());
        }
    }

    //嵌套创建目录
    @Test
    public void mkdir() throws IOException {
        fs.mkdirs(new Path("/ccc/ddd/eee/fff"));
        fs.close();
    }

    //递归删除目录和文件
    @Test
    public void delete() throws IOException {
        fs.delete(new Path("/ccc"), true);
        fs.close();
    }







    //查看文夹下所有的文件信息
    @Test
    public void fileStatus() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("blocksize: " + fileStatus.getBlockSize());
            System.out.println("owner: " + fileStatus.getOwner());
            System.out.println("Replication: " + fileStatus.getReplication());
            System.out.println("permission: " + fileStatus.getPermission());
            System.out.println("name: " + fileStatus.getPath().getName());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-len: " + bl.getLength() + "---" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }

            }
            System.out.println("---------------------------------------");
        }
    }

    //查看文件信息
    @Test
    public void infoDri() throws IOException, InterruptedException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus file : fileStatuses) {
            System.out.println("name: " + file.getPath().getName());
            System.out.println("permission2: " + file.getPath());
            System.out.println(file.isDir() ? "directory" : "file");
            System.out.println("----------------------------------------------");
        }
    }

    //流的方式上传文件
    @Test
    public void uploadStream() throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path("/ccc/ddd/aaa.jpg"));
        FileInputStream inputStream = new FileInputStream("F:\\aa.jpg");
        IOUtils.copy(inputStream, outputStream);
    }

    @Test
    public void readGzFile() throws IOException {
        Path file = new Path("/telephone-partitoner/part-r-00001.gz");
        FSDataInputStream hdfsInstream = fs.open(file);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(file);
        BufferedReader reader = null;
        try{
            if(codec == null){
                reader = new BufferedReader(new InputStreamReader(hdfsInstream));
            }else{
                CompressionInputStream comInStream = codec.createInputStream(hdfsInstream);
                IOUtils.copy(comInStream, System.out);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    //下载文件
    @Test
    public void downloadStream() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/ccc/aaa.txt"));
        FileOutputStream outputStream = new FileOutputStream("F:\\cccdd.txt");
//        IOUtils.copy(inputStream, outputStream);
//        IOUtils.copyLarge(inputStream, System.out, 0,inputStream.available());
        inputStream.seek(1000);
        org.apache.hadoop.io.IOUtils.copyBytes(inputStream, System.out, 4096,true);
    }

//    public static void main(String[] args) {
//        BB bb = new BB();
//        AA aaa = (AA)Proxy.newProxyInstance(BB.class.getClassLoader(), BB.class.getInterfaces(), (proxy, method, args1) -> {
//            System.out.println("aaa");
//            return method.invoke(bb, args1);
//        });
//        aaa.get();
//    }
}
interface AA{
    void get();
}
class BB implements AA{

    @Override
    public void get() {
        System.out.println("ccc");
    }
}