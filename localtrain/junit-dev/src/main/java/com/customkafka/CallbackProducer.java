package com.customkafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CallbackProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "104.250.147.226:6667,104.250.150.18:6667,104.250.141.66:6667");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 3);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String chatRecords = "{\"pageId\":\"1123345\",\"senderId\":\"xxx\",\"recipient\":\"xxx\",\"timestamp\":1593530874565,\"userId\":22}";

        producer.send(new ProducerRecord<String, String>("chat-records", chatRecords), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("success->" + metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    }
                });
        //for (int i = 0; i < 100; i++) {
        //    producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)), new Callback() {
        //        @Override
        //        public void onCompletion(RecordMetadata metadata, Exception exception) {
        //            if (exception == null) {
        //                System.out.println("success->" + metadata.offset());
        //            } else {
        //                exception.printStackTrace();
        //            }
        //        }
        //    });
        //}
        producer.close();
    }
}
