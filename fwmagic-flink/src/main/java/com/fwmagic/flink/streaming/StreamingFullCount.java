package com.fwmagic.flink.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class StreamingFullCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Long>> mapds = ds.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                if (value.startsWith("1")) {
                    return Tuple2.of("a", Long.parseLong(value));
                } else {
                    return Tuple2.of("b", Long.parseLong(value));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = mapds.keyBy(0);
        keyedStream.timeWindow(Time.seconds(5))
                //一次性迭代窗口中的所有数据：Iterable<Tuple2<String, Long>> elements
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                System.err.println("===>执行process。。。");
                List<Tuple2<String, Long>> list = new ArrayList<>();
                for (Tuple2<String, Long> e : elements) {
                    list.add(e);
                }
                out.collect("window: " + context.window() + "type: "+tuple.getField(0)+" \nlist: " + list);
            }
        }).print().setParallelism(1);

        env.execute("StreamingFullCount demo");
    }
}