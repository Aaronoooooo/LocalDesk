package com.fwmagic.flink.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Kafka2FlinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000);
        Map properties = new HashMap();
        //properties.put("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "local-consumer-group");
        properties.put("enable.auto.commit", "true");
       // properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        //properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
       // properties.put("topic","trackingData-2");
        List<String> topicList = new ArrayList<>();
        /*topicList.add("dev_trackingDataProcess");
        topicList.add("DrivingBehaviorData");
        topicList.add("userTracking");
        topicList.add("trackingData-2");*/
        //properties.put("topic",topicList);
        topicList.add("demo222");

        ParameterTool parameterTool = ParameterTool.fromMap(properties);

        FlinkKafkaConsumer010<ObjectNode> myConsumer = new FlinkKafkaConsumer010<>
                (topicList, new JSONKeyValueDeserializationSchema(true), parameterTool.getProperties());
        DataStreamSource<ObjectNode> source = env.addSource(myConsumer);
        source.map(new MapFunction<ObjectNode, String>() {
            @Override
            public String map(ObjectNode jsonNodes) throws Exception {
                JsonNode metadata = jsonNodes.get("metadata");
                String string = metadata.get("topic").toString();
                System.out.println("metadata: "+metadata);
                System.out.println("string: "+string);
                return string;
            }
        });
        source.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
