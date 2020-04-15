package com.fwmagic.flink.examplecase;

import com.alibaba.fastjson.JSON;
import com.fwmagic.flink.pojo.Student;
import com.fwmagic.flink.sink.MySqlBatchSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 消费kafka,解析批量插入到mysql
 */
public class ConsumerKafkaToMysqlBatch {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group3");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //earliest,lastest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(Arrays.asList("student"), new SimpleStringSchema(), props);

        DataStream<Student> dataStream = env.addSource(kafkaConsumer011).map(student -> JSON.parseObject(student, Student.class));

        //每一分钟收集一波数据，触发批量入库操作
        dataStream.timeWindowAll(Time.minutes(1)).process(new ProcessAllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Student> students, Collector<List<Student>> out) throws Exception {
                ArrayList<Student> list = Lists.newArrayList(students);
                if (!CollectionUtils.isEmpty(list)) {
                    if (list.size() > 0) {
                        System.err.println("process 1分钟收到的student数据条数是：" + list.size());
                        out.collect(list);
                    }
                }
            }
        }).addSink(new MySqlBatchSink());

        //每一分钟收集一波数据，触发批量入库操作
        /*dataStream.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Student> students, Collector<List<Student>> out) throws Exception {
                System.err.println("===>window[ start: "+window.getStart() +",end: "+window.getEnd()+",maxTimestamp: "+window.maxTimestamp());
                ArrayList<Student> list = Lists.newArrayList(students);
                if (!CollectionUtils.isEmpty(list)) {
                    if (list.size() > 0) {
                        System.out.println("apply 1分钟收到的student数据条数是：" + list.size());
                        out.collect(list);
                    }
                }
            }
        }).addSink(new MySqlBatchSink());*/
        env.execute("ConsumerKafkaToMysql batch operator!");
    }
}
