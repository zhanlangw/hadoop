package com.example.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class Server implements MyServer {
    @Override
    public String getDate(String data) {
        return "我来了" + data;
    }

//    public static void main(String[] args) throws IOException {
//        RPC.Builder builder = new RPC.Builder(new Configuration());
//        RPC.Server server = builder.setBindAddress("localhost").setPort(8888).setProtocol(MyServer.class).setInstance(new Server()).build();
//        server.start();
//    }
}
interface MyServer{
    public static final long versionID=1L;
    String getDate(String data);
}
