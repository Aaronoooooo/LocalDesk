package com.fwmagic.flink.streaming;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用flink对指定窗口内的数据进行实时统计，最终把结果打印出来
 * <p>
 * nc -lk 9999
 */

public class StreamingWindowWordCountJava {

    public static void main(String[] args) throws Exception {

        //定义socket的端口号，默认9999
        final int port;
        try {
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port", 9999);
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(3);
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port, "\n");
        //计算数据
        //拍平操作，把每行的单词转为<word,count>类型的数据
        DataStream<Tuple2<String, Long>> windowCount = text.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
            //针对相同的word数据进行分组
        }).keyBy(0)
                //指定计算数据的滚动窗口大小或滑动窗口大小，每1秒计算一次最近5秒窗口内的结果
                //.timeWindow(Time.seconds(5), Time.seconds(1))
                //每5秒计算一次窗口内的结果
                .timeWindow(Time.seconds(5))
                //每3条计算一次
                //.countWindow(3)
                .sum(1);

        //把数据打印到控制台,使用一个并行度
        windowCount.print().setParallelism(1);

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count!");
    }
}