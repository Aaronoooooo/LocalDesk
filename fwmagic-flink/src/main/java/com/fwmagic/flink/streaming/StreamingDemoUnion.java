package com.fwmagic.flink.streaming;

import com.fwmagic.flink.datasource.OneParalleDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingDemoUnion {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text1 = env.addSource(new OneParalleDataSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        DataStreamSource<Long> text2 = env.addSource(new OneParalleDataSource()).setParallelism(1);

        //把text1和text2组装到一起
        DataStream<Long> unionText = text1.union(text2);

        DataStream<Long> num = unionText.map(value -> {
            System.out.println("接收到的原始数据：" + value);
            return value;
        });


        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        env.execute("stream union demo");

    }
}
