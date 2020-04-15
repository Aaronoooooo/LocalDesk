package com.fwmagic.flink.examplecase;

import com.fwmagic.flink.sink.MySqlTwoPhaseNewCommitSink;
import com.fwmagic.flink.sink.MySqlTwoPhaseNewCommitSink2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

@SuppressWarnings("all")
public class ConsumerKafkaToMysqlExactlyOnce2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///Users/fangwei/temp/cp/"));

        //设置kafka消费参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fg");

        FlinkKafkaConsumer011<ObjectNode> consumer011 = new FlinkKafkaConsumer011("student", new JSONKeyValueDeserializationSchema(true), props);
        consumer011.setStartFromLatest();
        SingleOutputStreamOperator<ObjectNode> operator = env.addSource(consumer011);

        operator.addSink(new MySqlTwoPhaseNewCommitSink());

        env.execute("ConsumerKafkaToMysqlExactlyOnce case!");

    }
}
