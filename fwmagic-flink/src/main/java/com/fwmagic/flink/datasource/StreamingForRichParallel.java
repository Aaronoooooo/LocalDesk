package com.fwmagic.flink.datasource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingForRichParallel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> dataSource = env.addSource(new RichParalleDataSource()).setParallelism(3);

        DataStream<Long> dataStream = dataSource.map(value -> {
            System.out.println(Thread.currentThread().getId()+":接收的数据：value:" + value);
            return value;
        });

        dataStream.timeWindowAll(Time.seconds(5)).sum(0).print().setParallelism(1);

        env.execute(StreamingForRichParallel.class.getName());
    }
}
