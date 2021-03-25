package com.zongteng.ztetl.common.kafka.offset

import kafka.api.OffsetRequest
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.zookeeper.data.Stat

import scala.collection.mutable


/**
  * 通过zk手动维护kafka偏移量，两个功能：
  * 1、读取offset
  * 2、更新offset
  */
object KafkaOffsetManager extends Serializable {

  // 1、通过消费者组和topic，获取这个topic所有分区的偏移量 完成

  // 2、提交一个rdd包含的所有topic和分区的最新偏移量

  // 3、

  /***
    *
    * zookeeper操作命令：\
    *
    * 登录
    * cd /opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/zookeeper/bin
    * ./zkCli.sh -server zongteng72:2181,zongteng73:2181,zongteng74:2181
    *
    * 查看目录：ls 路径
    *
    * 获取数据：/sparkStreaming_kafka_offset_manager/consumer/topic/partition/分区号/偏移量
    *
    * 创建一个持久化节点 并且设置偏移量为100。要递归地创建节点，下一个节点依赖上个节点的存在
    * create /sparkStreaming_kafka_offset_manager/consumer/topic/partition/0 100
    *
    * 测试：
    * create /sparkStreaming_kafka_offset_manager 100
    * set /sparkStreaming_kafka_offset_manager 200
    * set /sparkStreaming_kafka_offset_manager 300
    *
    * get /sparkStreaming_kafka_offset_manager
    *
     kafka操作命令





    拓展分区
    kafka-topics \
    --alter \
    --partitions 4 \
    --topic kafkaOffset \
    --zookeeper zongteng72:2181,zongteng73:2181,zongteng74:2181
    *
    * 1、路径
    * create /kafka_offset_manager/consumers/consumer_group/gc_wms/kafkaOffset_0 100
    * create /kafka_offset_manager/consumers/consumer_group/gc_wms/kafkaOffset_1 200
    * create /kafka_offset_manager/consumers/consumer_group/gc_wms/kafkaOffset_2 300
    *
    *
      create /kafka_offset_manager/consumers/consumer_group/gc_wms/test_1 100
      create /kafka_offset_manager/consumers/consumer_group/gc_wms/test_2 200
      create /kafka_offset_manager/consumers/consumer_group/gc_wms/test_3 300
    *
    * ls /kafka_offset_manager/consumers/consumer_group/gc_wms
    *
    */

  lazy val log = org.apache.log4j.LogManager.getLogger("KafkaOffsetManage")

  /**
    * 读取offset：通过消费者组Id和topic，获取这个topic所有分区的偏移量
    * @param zkutils zkutils客户端对象
    * @param zkOffsetPath zk偏移量路径
    * @param topic 主题
    */
  def readOffset(zkutils: ZkUtils, zkOffsetPath: String, topic: Seq[String]) = {

    // 节点不存在
    if (!zkutils.pathExists(zkOffsetPath)) {
      println("新的消费者第一次进来")
      None
    } else {

      println("消费者获取最新提交的偏移量")
      // 总的：获取分区的最近一次提交偏移量，如果分区是新增的那么为0

      // 获取订阅主题所有的分区，格式：主题-分区（kafkaOffset-0）
      val topicParts: mutable.Iterable[String] = zkutils.getPartitionsForTopics(topic).flatMap((topicPartitions: (String, Seq[Int])) => {
        val topic: String = topicPartitions._1
        val partitions: Seq[Int] = topicPartitions._2
        partitions.map((topic + "-" + _))
      })

      // 获取订阅主题所有分区的偏移量的，如果是新增那么为0
      val topicAndPartition_Offset: Map[TopicPartition, Long] = topicParts.map(topicPart => {
        val partitionPath = zkOffsetPath + "/" + topicPart

        //  通过节点路径和主题，获取消费者消费对应主题所有分区的偏移量
        val tuple: (Option[String], Stat) = zkutils.readDataMaybeNull(partitionPath)

        // 如果匹配获取上次提交的偏移量，没有匹配证明是新增的为0
        val offset: Int = tuple._1 match {
          case Some(offsetsRangesStr) => offsetsRangesStr.toInt
          case None => 0
        }

        // 返回((主题, 分区), 偏移量)
        val tp: String = topicPart.split("-")(0)
        val pt: Int =  topicPart.split("-")(1).toInt
        (new TopicPartition(tp, pt), offset.toLong)
        // (TopicAndPartition(tp, pt), offset.toLong)
      }).toMap

      topicAndPartition_Offset.foreach(println)
      Some(topicAndPartition_Offset)
    }
  }

  /**
    *
    * @param zkutils
    * @param zkClient
    * @param zkOffsetPath  消费者组路径 /kafka-offset-manager/consumers/consumer-group/consumer-group-name
    */
  def savaOffsets(zkutils: ZkUtils, zkClient: ZkClient, zkOffsetPath: String, rdd: RDD[_]) = {

    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    println("savaOffsets")

    // 判断消费者是否存在

    // 不存在创建消费者节点、和主题分区节点（存储偏移量）
    if (!zkutils.pathExists(zkOffsetPath)) {
      zkClient.createPersistent(zkOffsetPath, zkOffsetPath)
      zkutils.readDataMaybeNull(zkOffsetPath)._1 match {
        case Some(value) => println(s"消费者组节点{$value}创建成功")
        case None => throw new Exception(s"消费者组节点{$zkOffsetPath}节点创建失败")
      }
    }

    // 遍历创建多个主题分区节点，如果不存在，创建节点并更新值。如果存在，直接更新值
    offsetRanges.foreach((offsetRange: OffsetRange) => {
      val nodePath = zkOffsetPath + "/" + (offsetRange.topic + "-" + offsetRange.partition)
      println("nodePath == " + nodePath + " offset == " + offsetRange.untilOffset)

      if (!zkutils.pathExists(nodePath)) {
        zkClient.createPersistent(nodePath, offsetRange.untilOffset.toString)
        println("新建-分区偏移量节点，赋值offset nodePath == " + nodePath + " offset == " + offsetRange.untilOffset)
      } else {
        zkutils.updatePersistentPath(nodePath, offsetRange.untilOffset.toString)
        println("更新-分区偏移量节点，更新offset值 nodePath == " + nodePath + " offset == " + offsetRange.untilOffset)
      }

    })


//
//
//    // 创建对应的分区节点 里面存放偏移量的值
//    offsetRanges.map(_.t)
//
//    // 如果存在那么提交
//
//    offsetRanges.

  }





  def main(args: Array[String]): Unit = {
    // 1、获取ZkClient对象
   val tuple: (ZkClient, ZkConnection) = ZkUtils.createZkClientAndConnection("zongteng72:2181,zongteng73:2181,zongteng74:2181", 30000, 30000)

    val zkutils: ZkUtils = new ZkUtils(tuple._1, tuple._2, true)

    val zkOffsetPath = "/kafka_offset_manager/consumers/consumer_group/gc_wms"
    val topic = "kafkaOffset"

    val topic2 = "kafkaOffset2"
    val topi3 = "kafkaOffset1"
    readOffset(zkutils, zkOffsetPath, Seq(topic, topic2, topi3)) match  {
      case Some(e) => println(e)
      case None => println("没有匹配")
    }

    //savaOffsets(zkutils, tuple._1, zkOffsetPath)



//    // 1、配置kafka消费者
//    val kafkaOptions = new Properties()
//    kafkaOptions.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "zongteng73:9092,zongteng74:9092,zongteng75:9092")
//    kafkaOptions.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaOffset")
//    kafkaOptions.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer" )
//    kafkaOptions.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
//
//    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaOptions)
//
//    // 订阅主题
//    consumer.subscribe(Collections.singletonList("gc_wms"))
//
//    // 消费数据
//    while (true) {
//      val records: ConsumerRecords[String, String] = consumer.poll(100)
//      val recordsIterators: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
//
//      while (recordsIterators.hasNext) {
//        val record: ConsumerRecord[String, String] = recordsIterators.next()
//        // println(record.value())
//
//
//
//        println("==========offset===========" + record.offset())
//
//      }
//
//
//    }

//    println(classOf[ContainerDetails])
//    println(Class.forName("com.zongteng.ztetl.entity.gc_lms_au.ContainerDetails"))
//    println(ContainerDetails.getClass)

  }





}
