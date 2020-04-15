package com.fwmagic.flink.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class StreamingToRedisSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9998);

        //对数据进行封装，把String 转为Tuple2<String,String>
        DataStream<Tuple2<String, String>> dataStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return Tuple2.of("fw_word", value);
            }
        });

        //redis配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

        //create redisSink
        RedisSink redisSink = new RedisSink<>(config, new MyRedisMapper());

        dataStream.addSink(redisSink);

        env.execute("redis sink demo!");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getKeyFromData(Tuple2<String, String> tuple) {
            System.out.println("f0:"+tuple.f0);
            return tuple.f0;
        }

        //表示从接收的数据中获取需要操作的redis key
        @Override
        public String getValueFromData(Tuple2<String, String> tuple) {
            System.out.println("f1:"+tuple.f1);
            return tuple.f1;
        }

    }

}