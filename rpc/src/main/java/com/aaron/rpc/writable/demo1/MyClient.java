package com.aaron.rpc.writable.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Aaron
 * @version V1.0
 * @Package com.aaron.rpc.writable.demo1
 * @date 2021/8/7 11:06
 */
public class MyClient {
    public static void main(String[] args) {
        try {
            BussinessProtocol proxy = RPC.getProxy(BussinessProtocol.class,
                    BussinessProtocol.versionID,
                    new InetSocketAddress("localhost", 6789),
                    new Configuration());

            proxy.mkdir("./bigdata/rpctest");
            String rpcResult = proxy.getName("bigdata");
            System.out.println("从 RPC 服务端接收到的 getName RPC 请求的响应结果: " + rpcResult);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
