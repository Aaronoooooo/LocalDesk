package com.zongteng.ztetl.api

import com.zongteng.ztetl.common.kafka.offset.KafkaOffsetManager
import com.zongteng.ztetl.common.zookeeper.ZkConf
import com.zongteng.ztetl.util.PropertyFile
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkKafkaCon {

  def getConnectKafka(appName: String, inverval: Int, topic: String, groupid: String) : (StreamingContext, InputDStream[ConsumerRecord[String, String]]) = {
    getConnectKafka(appName, inverval,  Array[String](topic), groupid)
  }

  def getConnectKafka(appName: String, inverval: Int, topics: Array[String], groupid: String) : (StreamingContext, InputDStream[ConsumerRecord[String, String]]) = {

    val  kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> PropertyFile.getProperty("kafka_sesrver"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
   //  "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    getConnectKafka(appName, inverval, topics, kafkaParams)
  }

  def getConnectKafka(appName: String, inverval: Int, topics: Array[String], kafkaParams: Map[String, Object]) : (StreamingContext, InputDStream[ConsumerRecord[String, String]])   ={

    val streamingContext: StreamingContext = SparkConfig.getStreamingContext(appName, inverval)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    (streamingContext, kafkaDStream)
  }



  def getConnectKafka(appName: String,
                      interval: Int,
                      sparkConf: org.apache.spark.SparkConf,
                      topics: Array[String],
                      kafkaParams: Map[String, Object]
                     ): (StreamingContext, InputDStream[ConsumerRecord[String, String]]) = {

    val streamingContext: StreamingContext = SparkConfig.getStreamingContext(appName, interval, sparkConf)

    // 不能获取上次提交的偏移量，重启之后依靠策略latest、earliest执行
    val kafkaInputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    (streamingContext, kafkaInputDStream)
  }


  /**
    * 获取(SparkStream, KafkaStrem, ZkClient, ZkUtils)元祖，通过offset
    * 默认使用这个，根据业务需要，重载这个方法
    * @param appName
    * @param interval
    * @param groupId
    * @param topics
    * @param isEarliest 是否从头开始消费topic，从topic每个分区的第一条数据开始，默认false
    * @return
    */
  def getSparkStream$KafkaStrem$ZkClient$ZkUtilsByOffset(appName: String, interval: Int, groupId: String, topics: Array[String], isEarliest: Boolean = false) = {

    // spark配置参数
    val sparkConf = new SparkConf()

    // 反压参数
    sparkConf.set("spark.streaming.backpressure.enabled", "true") // 激活削峰（反压）功能
    sparkConf.set("spark.streaming.backpressure.initialRate","1000")// 第一次读取的最大数据值
    sparkConf.set("spark.streaming.receiver.maxRate", "1000") // 每个receiver 每秒最大可以接收的记录的数据
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000") // 限制每次作业中每个Kafka分区最多读取的记录条数（这个有效 5倍）

    // kafka配置参数
    var kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> PropertyFile.getProperty("kafka_sesrver"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      //"auto.offset.reset" -> "latest",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // zookeeper
    val tuple2: (ZkClient, ZkConnection) = ZkConf.getZkClientAndConnection(30000, 30000)
    val zkClient: ZkClient = tuple2._1
    val zkConnection: ZkConnection = tuple2._2
    val zkUtils: ZkUtils = new ZkUtils(zkClient, zkConnection, true)

    // 这个节点下，存放主题分区节点，主题分区节点存放偏移量。相当于主题分区节点的父节点
    val zkOffsetPath = "/kafka_offset_manager/consumers/consumer_group/" + groupId

    // 读取偏移量信息
    val partitionToLongOption: Option[Map[TopicPartition, Long]]= KafkaOffsetManager.readOffset(zkUtils, zkOffsetPath, topics)

    val sparkStreamAndKafkaStrem = if (!isEarliest && !partitionToLongOption.isEmpty) {
      SparkKafkaCon.getConnectKafka(appName, interval, sparkConf, topics, kafkaParams, partitionToLongOption.get) // 消费者已经启动过，获取最后一次提交的偏移量
    } else {
      // 消费者第一次启动
      println("消费者第一次启动")
      kafkaParams += ("auto.offset.reset" -> "earliest")
      SparkKafkaCon.getConnectKafka(appName, interval, sparkConf, topics, kafkaParams)
    }

    // 返回结果
    (sparkStreamAndKafkaStrem._1, sparkStreamAndKafkaStrem._2, zkClient, zkUtils, zkOffsetPath)
  }

  def getConnectKafka(appName: String, interval: Int, sparkConf: org.apache.spark.SparkConf, topics: Array[String],
                      kafkaParams: Map[String, Object],
                      offsets: Map[TopicPartition, Long]
                     ): (StreamingContext, InputDStream[ConsumerRecord[String, String]]) = {

    val streamingContext: StreamingContext = SparkConfig.getStreamingContext(appName, interval, sparkConf)

    // 可以获取上次提交的偏移量，重启之后依靠offsets执行
    val kafkaInputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
    )

    (streamingContext, kafkaInputDStream)
  }

  def getConnectKafka(streamingContex:StreamingContext, topic_name:String, groupid:String) : InputDStream[ConsumerRecord[String, String]] ={
    val  kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertyFile.getProperty("kafka_sesrver"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic_name)

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      streamingContex,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    kafkaDStream
  }



}
