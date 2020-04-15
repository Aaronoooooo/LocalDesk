package com.fwmagic.flink.streaming;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 消费kafka消息，sink(自定义)到mysql中，保证kafka to mysql的Exactly-Once
 */
@SuppressWarnings("all")
public class StreamDemoKafka2Mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.getConfig().registerTypeWithKryoSerializer(Connection.class, DefaultSerializers.KryoSerializableSerializer.class);
        //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有10s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        //检查点必须在1分钟之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        //env.setStateBackend(new FsStateBackend("file:///Users/fangwei/temp/cp/"));
        //设置kafka消费参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hd1:9092,hd2:9092,hd3:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group1");

        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
         //FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        FlinkKafkaConsumer010<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer010<>("gaga", new JSONKeyValueDeserializationSchema(true), props);
        kafkaConsumer011.setStartFromLatest();
        //加入kafka数据源
        DataStreamSource<ObjectNode> streamSource = env.addSource(kafkaConsumer011).setParallelism(1);
        streamSource.print().setParallelism(1);
        //数据传输到下游
       // streamSource.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink2").setParallelism(1);
        //触发执行
        env.execute(StreamDemoKafka2Mysql.class.getName());

    }
}
