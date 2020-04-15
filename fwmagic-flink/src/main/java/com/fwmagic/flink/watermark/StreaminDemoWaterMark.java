package com.fwmagic.flink.watermark;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 测试数据
 * aa 1557547200000
 * aa 1557547205000
 * aa 1557547210000
 * aa 1557547219000
 * aa 1557547226000
 * aa 1557547227000
 * aa 1557547228000
 * aa 1557547800000
 * <p>
 * 结果：
 (aa,1557547200000,4)
 (aa,1557547226000,3)
 */
public class StreaminDemoWaterMark {

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

        //每5秒发出一个watermark
        // env.getConfig().setAutoWatermarkInterval(10000);

        DataStream<String> ds = env.socketTextStream("localhost", 9000);

        ds.filter(new FilterClass()).map(new LineSplitter())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {

                    //当前最大时间戳
                    private long currentMaxTimeStamp = 0L;

                    //控制失序已经延迟的度量（10s）
                    private final long maxOutOfOrderness = 10000L;

                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Long timestamp = element.f1;
                        currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp);
                        System.err.println("==>获取用户输入的时间 :" + sdf.format(new Date(timestamp)) + "(" + timestamp + ")" + " 当前最大时间 :" + "(" + currentMaxTimeStamp + ")");
                        //System.out.printf("get timestamp is :%d, currentMaxTimeStamp is: %d",timestamp,currentMaxTimeStamp);
                        return timestamp;
                    }

                    //获取watermark
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        long time = System.currentTimeMillis();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Long t = currentMaxTimeStamp - maxOutOfOrderness;
                        System.err.println("(" + time + ")" + sdf.format(new Date(time)) + " 触发 watermark ---> 新的 watermark是: " + "(" + (t) + ")");
                        return new Watermark(t);
                    }
                }).keyBy(0).timeWindow(Time.seconds(20)).sum(2).print();

        env.execute("watermark demo!");

    }

}
