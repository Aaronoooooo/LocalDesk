package com.fwmagic.flink;

import org.apache.flink.table.descriptors.Kafka;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;

public class MyKafkaConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hd1:9092,hd2:9092,hd3:9092");
        //props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "400000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "300000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"fwmagic_consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("demo123"), new MyConsumerRebalanceListener());

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(5000);
            for(ConsumerRecord<String,String> record:records){
                int partition = record.partition();
                System.out.println("分区partition："+partition);
                System.out.println("当前分区偏移量为："+record.offset());
                System.out.println("key:"+record.key()+" -> value:"+record.value());
            }
        }
        //同步提交
        //consumer.commitSync();

    }


}
class MyConsumerRebalanceListener implements ConsumerRebalanceListener{

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println("kafka Rebalance start!!!");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println(new Date().toString() + " kafka Rebalance  end!!!");

    }
}