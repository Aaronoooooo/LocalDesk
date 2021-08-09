package com.aaron.rpc.writable.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import java.io.IOException;

/**
 * @author Aaron
 * @version V1.0
 * @Package com.aaron.rpc.writable.demo1
 * @date 2021/8/7 10:58
 */
public class MyServer {
    public static void main(String[] args) {
        try {
            RPC.Server server = new RPC.Builder(new Configuration())
                    .setProtocol(BussinessProtocol.class)
                    .setInstance(new BusinessIMPL())
                    .setBindAddress("localhost")
                    .setPort(6789)
                    .build();

            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
