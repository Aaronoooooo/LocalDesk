package com.mobikok.bigdata.spark.streaming.oper

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkStreaming06_Kafka {

  def main(args: Array[String]): Unit = {

    // TODO Spark环境
    // SparkStreaming使用核数最少是2个
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 使用SparkStreaming读取Kafka的数据

    // Kafka的配置信息一

    //val kafkaPara: Map[String, Object] = Map[String, Object](
    //    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bigdata1:9092,bigdata2:9092,bigdata3:9092",
    //    ConsumerConfig.GROUP_ID_CONFIG -> "mobikok",
    //    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    //    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    //)
    //val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
    //    KafkaUtils.createDirectStream[String, String](
    //        ssc,
    //        LocationStrategies.PreferConsistent,
    //        ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaPara)
    //    )
    // Kafka的配置信息二
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata1:9092,bigdata2:9092,bigdata3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g101",
      //            "auto.offset.reset" -> "latest",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    //        val topics = Array("chat-records", "test")
    val topics = Array("chat-records")
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
//      valueDStream.map(record => )
//spark.createDataFrame()
    //        valueDStream.

    valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()


    ssc.start()
    // 等待采集器的结束
    ssc.awaitTermination()


  }
}
