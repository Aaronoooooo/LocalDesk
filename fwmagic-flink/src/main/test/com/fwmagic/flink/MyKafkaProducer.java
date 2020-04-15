package com.fwmagic.flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        for(int i=0;i<10;i++){
            Thread.sleep(1000);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("demo123", i+"");
            kafkaProducer.send(producerRecord);
            System.out.println("send data:"+i);
        }

        kafkaProducer.close();
    }
}
