package com.fwmagic.flink.examplecase;

import com.alibaba.fastjson.JSON;
import com.fwmagic.flink.pojo.Student;
import com.fwmagic.flink.sink.MySqlSingleSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消费kafka,解析插入到mysql
 */
public class ConsumerKafkaToMysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //earliest,lastest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(Arrays.asList("student"), new SimpleStringSchema(), props);

        DataStream<Student> dataStream = env.addSource(kafkaConsumer011).map(msg -> JSON.parseObject(msg, Student.class));

        dataStream.addSink(new MySqlSingleSink());

        env.execute("ConsumerKafkaToMysql single operator!");

    }
}
