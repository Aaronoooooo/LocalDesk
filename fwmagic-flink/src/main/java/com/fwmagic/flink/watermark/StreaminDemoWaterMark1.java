package com.fwmagic.flink.watermark;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * WaterMark测试
 */
public class StreaminDemoWaterMark1 {

    //自定义过滤器，过滤出不为空的字符串
    public static final class FilterClass implements FilterFunction<String> {
        @Override
        public boolean filter(String s) throws Exception {
            return StringUtils.isNotBlank(s);
        }
    }

    //构造出element以及它的event time.然后把次数赋值为1
    public static final class LineSplitter implements MapFunction<String, Tuple3<String, Long, Integer>> {
        @Override
        public Tuple3<String, Long, Integer> map(String value) throws Exception {
            String[] arr = value.split("\\W+");
            long eventime = Long.parseLong(arr[1]);
            return Tuple3.of(arr[0], eventime, 1);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //事件(消息)发生的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        //每5秒发出一个watermark,默认100ms发送一次
        //env.getConfig().setAutoWatermarkInterval(5000);

        DataStream<String> ds = env.socketTextStream("localhost", 9000);

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> watermarks = ds.filter(new FilterClass()).map(new LineSplitter())
                //periodic、punctuated
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {

                    //当前最大时间戳
                    private long currentMaxTimestamp = 0L;

                    //控制失序已经延迟的度量（10s）
                    private final long maxOutOfOrderness = 10000L;

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                    /**
                     * 有数据输入则触发
                     * @param element
                     * @param previousElementTimestamp
                     * @return
                     */
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
                        Long timestamp = element.f1;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        long id = Thread.currentThread().getId();

                        System.err.println("currentThreadId:" + id + ",key:" + element.f0 + ",eventtime:[" + element.f1 + "|" + sdf.format(element.f1) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" +
                                sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                        return timestamp;
                    }

                    //获取watermark，默认100ms触发一次
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                });

        //将迟到被丢弃的数据保存起来
        OutputTag<Tuple3<String, Long, Integer>> outputTag = new OutputTag<Tuple3<String, Long, Integer>>("later-data") {
        };


        SingleOutputStreamOperator<String> outputStreamOperator = watermarks.keyBy(0)
                .timeWindow(Time.seconds(3))
                //.allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple3<String, Long, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> it, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        Iterator<Tuple3<String, Long, Integer>> iterator = it.iterator();
                        List<Long> list = new ArrayList<>();
                        while (iterator.hasNext()) {
                            Tuple3<String, Long, Integer> next = iterator.next();
                            list.add(next.f1);
                        }

                        //对时间排序
                        Collections.sort(list);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String result = key + "," + list.size() + "," + sdf.format(list.get(0))
                                + "," + sdf.format(list.get(list.size() - 1))
                                + "," + window.getStart() + "," + window.getEnd();

                        out.collect(result);
                    }
                });

        DataStream<Tuple3<String, Long, Integer>> sideOutput = outputStreamOperator.getSideOutput(outputTag);
        //迟到被丢弃的数据
        sideOutput.print();

        //正常处理的数据
        outputStreamOperator.print().setParallelism(1);

        env.execute("watermark demo1!");

    }

}
