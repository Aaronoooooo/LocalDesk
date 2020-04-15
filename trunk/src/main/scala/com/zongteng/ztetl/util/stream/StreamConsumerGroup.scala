package com.zongteng.ztetl.util.stream

import com.zongteng.ztetl.common.kafka.offset.KafkaOffsetManager
import com.zongteng.ztetl.common.zookeeper.ZkConf
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.common.TopicPartition

object StreamConsumerGroup {

  val CONSUMER_GC_OMS = "consumer_gc_wms"

  val CONSUMER_GC_TCMS = "consumer_gc_tcms"

  val CONSUMER_GC_WMS = "consumer_gc_wms"

  val CONSUMER_GC_WMS_IBL = "consumer_gc_wms_ibl"

  /**
    * 查看所有消费者目前的offset
    * @return
    */
  def queryAllConsumerOffset() = {
    // zookeeper
    val tuple2: (ZkClient, ZkConnection) = ZkConf.getZkClientAndConnection(30000, 30000)
    val zkClient: ZkClient = tuple2._1
    val zkConnection: ZkConnection = tuple2._2
    val zkUtils: ZkUtils = new ZkUtils(zkClient, zkConnection, true)


    Array(CONSUMER_GC_OMS, CONSUMER_GC_TCMS, CONSUMER_GC_WMS, CONSUMER_GC_WMS_IBL).foreach(groupId => {
      // 这个节点下，存放主题分区节点，主题分区节点存放偏移量。相当于主题分区节点的父节点
      val zkOffsetPath = "/kafka_offset_manager/consumers/consumer_group/" + groupId


      val topics = Seq(groupId.replace("consumer_", ""))

      // 读取偏移量信息
      val partitionToLongOption: Option[Map[TopicPartition, Long]]= KafkaOffsetManager.readOffset(zkUtils, zkOffsetPath, topics)

      println(partitionToLongOption)
    })

  }

  def main(args: Array[String]): Unit = {
    queryAllConsumerOffset
  }




}
