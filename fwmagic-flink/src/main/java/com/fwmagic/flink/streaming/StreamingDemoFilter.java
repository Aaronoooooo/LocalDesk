package com.fwmagic.flink.streaming;

import com.fwmagic.flink.datasource.OneParalleDataSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingDemoFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new OneParalleDataSource());

        DataStream<Long> dataStream = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的value:"+value);
                return value;
            }
        });

        //filter过滤，满足条件的被留下（偶数）
        SingleOutputStreamOperator<Long> filterData = dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        SingleOutputStreamOperator<Long> result = filterData.map(data -> {
            System.out.println("过滤后的数据：data : " + data);
            return data;
        });

        //每2s处理一次
        result.timeWindowAll(Time.seconds(5) ).sum(0).print().setParallelism(1);

        env.execute("stream filter demo");
    }
}
