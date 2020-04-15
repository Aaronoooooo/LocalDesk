package com.fwmagic.flink.streaming;

import com.fwmagic.flink.datasource.OneParalleDataSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StreamingDemoSplit {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new OneParalleDataSource());

        DataStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    outPut.add("even");//偶数
                } else {
                    outPut.add("odd");//奇数
                }
                return outPut;
            }
        });

        DataStream<Long> even = ((SplitStream<Long>) splitStream).select("even");

        DataStream<Long> odd = ((SplitStream<Long>) splitStream).select("odd");

        ((SplitStream<Long>) splitStream).select("even", "odd");

        odd.print().setParallelism(1);
        env.execute("stream split demo");

    }
}
