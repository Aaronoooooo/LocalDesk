package com.fwmagic.flink.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class KafkaConsumerUtil {
    public static final String brokerServers = "localhost:9092";

    public static final String topic = "student";

    public static final String groupId = "g17";

    public static void main(String[] args) {
        consumerMessage();
    }

    public static void consumerMessage() {
        Properties prop = new Properties();
        //连接broker集群地址，多个broker逗号隔开
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        //是否自动ack
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //consumer向consunmer group发送心跳的超时时间，超过这个时间，consunmer group会将consunmer从组中移除，会发生rebalance.
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        //心跳时间一般要小于session timeout时间的1/3
        //prop.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        /**
         * 消费者加入group时，消费offset设置策略
         * earliest: 重置offset为最早的偏移地址，
         * latest: 重置ofsset为已经消费的最大偏移，
         * none: 没有offset时抛异常
         */
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //多久自动提交一次ack,单位：ms
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        //配置消费者所属的分组id
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //反序列化key
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //反序列化value
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //每次poll最多拉取多少条记录
        prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 8);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.err.printf("topic: %s, partition: %s, offset: %d,key: %s, value: %s \n", record.topic(), record.partition(),record.offset(), record.key(), record.value());
            }
        }
    }
}
