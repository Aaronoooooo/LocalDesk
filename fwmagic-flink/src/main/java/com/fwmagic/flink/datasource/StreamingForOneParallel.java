package com.fwmagic.flink.datasource;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义数据源处理
 */
public class StreamingForOneParallel {

    public static void main(String[] args) throws Exception {

        //设置StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
       // env.setParallelism(1);
        //添加自定义数据源
        DataStreamSource<Long> streamSource = env.addSource(new OneParalleDataSource());

        //处理获取的数据
        DataStream<Long> streamOperator = streamSource.map(value -> {
            System.out.println("=====>接收到的数据：" + value);
            return value;
        });

        //窗口函数操作
        streamOperator.timeWindowAll(Time.seconds(5))
                .sum(0)
                .print();

        //触发
        env.execute(StreamingForOneParallel.class.getName());
    }
}
