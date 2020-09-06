package com.flinkhbase

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.hadoop.hbase.client.HBaseAdmin


object KafkaToMySQLHbase {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "flydiysz.cn:31092")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "agdz6xsz")
    properties.setProperty("auto.offset.reset", "latest")
//    properties.setProperty("auto.offset.reset", "earliest");

    //生产者
    val produce1 = env.readTextFile("src/main/resources/hbase_pro.txt")
    produce1.addSink(new FlinkKafkaProducer[String]("test101", new SimpleStringSchema, properties))

    val input = env.addSource(new FlinkKafkaConsumer[String]("test101", new SimpleStringSchema(), properties)).setParallelism(1)
    input.print("topic").setParallelism(1)

    // 自定义MysqlSink类，将数据Sink到mysql
//    val sink = new MySQLSink("jdbc:mysql://bigdata1:3306/flinksink", "root", "spf@123")
//    input.addSink(sink)

    // 自定义HBaseSink类,将数据Sink到HBase(Student不会自动创建)
    val hBaseSink = new HBaseSink("teacher", "info")
    input.addSink(hBaseSink)

    env.execute("KafkaToSinkStreaming")
  }
}
