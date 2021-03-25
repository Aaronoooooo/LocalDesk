package com.mobikok.bigdata.spark.streaming.servicereplydelay.util;

import redis.clients.jedis.Jedis;

public class RedisClientTest {
    public static void main(String[] args) {
        //连接本地的 Redis 服务
        //Jedis jedis = new Jedis("r-6we1rqef1l307oteukpd.redis.japan.rds.aliyuncs.com");
        //Jedis jedis = new Jedis("r-wz9cnnzrmizj9xqyoepd.redis.rds.aliyuncs.com");
        Jedis jedis = new Jedis("8.209.242.69");
        jedis.auth("iX0TVWCZrA3jf6wT");
        //Jedis jedis = new Jedis("bigdata1");
        // 如果 Redis 服务设置来密码，需要下面这行，没有就不需要
        //查看服务是否运行
        System.out.println("服务正在运行: "+jedis.ping());
    }
}
