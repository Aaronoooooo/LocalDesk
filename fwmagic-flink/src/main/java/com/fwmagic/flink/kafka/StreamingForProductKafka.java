package com.fwmagic.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * flink发送kafka消息
 */
public class StreamingForProductKafka {

    public static void main(String[] args) throws Exception {
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
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hd1:9000/flink/checkpoints", true));


        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 6666,"\n");
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092");
        String topic = "demo123";

        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.split("\\s");
                for(String str:split){
                    collector.collect(Tuple2.of(str,1L));
                }
            }
        }).keyBy(0).sum(1).print().setParallelism(1);
        //Kafka生产者
        FlinkKafkaProducer011<String> kafkaProducer =
                new FlinkKafkaProducer011<>(topic,
                        new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                        prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        streamSource.addSink(kafkaProducer);
        //env.readFile(null, null).addSink(kafkaProducer);

        //启动
        env.execute(StreamingForProductKafka.class.getName());
    }
}
