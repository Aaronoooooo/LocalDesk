package com.fwmagic.flink.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;
import java.util.regex.Pattern;

public class StreamingForConsumeKafka {
    public static void main(String[] args) throws Exception {
        //构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint配置
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend，将检查点保存到hdfs上面
       // env.setStateBackend(new RocksDBStateBackend("hdfs://hd1:9000/flink/checkpoints", true));

        //设置kafka消费者参数
        Properties prop = new Properties();
        //prop.setProperty("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092");
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.setProperty("group.id", "group-1");


        //String topic = "demo123";
        Pattern pattern = Pattern.compile("demo123");
        //获取kafka偏移量:new JSONKeyValueDeserializationSchema(true)
        FlinkKafkaConsumer011<ObjectNode> kafkaConsumerSource = new FlinkKafkaConsumer011<>(pattern, new JSONKeyValueDeserializationSchema(true), prop);

        //kafkaConsumerSource.setStartFromGroupOffsets();//默认消费策略

        DataStreamSource<ObjectNode> source = env.addSource(kafkaConsumerSource).setParallelism(3);
        SingleOutputStreamOperator<Object> map = source.map(new MapFunction<ObjectNode, Object>() {
            @Override
            public Object map(ObjectNode objectNode) throws Exception {
                System.out.println("ObjectNode =>" + objectNode);
                JsonNode metadata = objectNode.get("metadata");
                String offsetStr = metadata.get("offset").toString();
                long offset = Long.parseLong(offsetStr);
                JsonNode topic = metadata.get("topic");
                JsonNode partition = metadata.get("partition");
                System.out.println(topic + "," + offset + "," + partition);
                return objectNode;
            }
        });

        //dataStreamSource.timeWindowAll(Time.seconds(5));
        //数据传输到下游(sink)
       // dataStreamSource.print().setParallelism(1);
        //dataStreamSource.timeWindowAll(Time.seconds(5));
        //dataStreamSource.addSink(new MyHttpSink());
        env.execute(StreamingForConsumeKafka.class.getName());
    }
}
