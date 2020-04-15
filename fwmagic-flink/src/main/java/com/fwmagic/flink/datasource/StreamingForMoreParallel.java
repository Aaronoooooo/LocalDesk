package com.fwmagic.flink.datasource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingForMoreParallel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置两个并行度
        DataStreamSource<Long> dataSource = env.addSource(new MoreParalleDataSource()).setParallelism(1);

        DataStream<Long> dataStream = dataSource.map(aLong -> {
            System.out.println("===>接收到的数据：" + aLong);
            return aLong;
        });

        //窗口函数操作
        dataStream.timeWindowAll(Time.seconds(5))
                .sum(0)
                .setParallelism(1)
                .print();

        env.execute(StreamingForMoreParallel.class.getName());

    }

}
