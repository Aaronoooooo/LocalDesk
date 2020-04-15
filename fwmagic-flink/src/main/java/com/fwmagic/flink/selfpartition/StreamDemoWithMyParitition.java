package com.fwmagic.flink.selfpartition;

import com.fwmagic.flink.datasource.OneParalleDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamDemoWithMyParitition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(20);

        DataStreamSource<Long> source = env.addSource(new OneParalleDataSource());

        //long -> Tuple<Long>
        DataStream<Tuple1<Long>> dataStream = source.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return Tuple1.of(value);
            }
        });

        //使用自定义分区
        DataStream<Tuple1<Long>> partitionData = dataStream.partitionCustom(new MyPartition(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> tuple1) throws Exception {
                System.out.println("当前线程id: " + Thread.currentThread().getId() + " value: " + tuple1);
                return tuple1.f0;
            }
        });

        result.print().setParallelism(2);

        env.execute("self partition!");

    }
}
