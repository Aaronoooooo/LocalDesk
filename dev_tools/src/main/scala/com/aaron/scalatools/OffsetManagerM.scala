package com.atguigu.gmall.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
  * Date: 2020/11/2
  * Desc: 从MySQL中读取偏移量的工具类
  */
object OffsetManagerM {

  //获取偏移量
  /**
    * 从Mysql中读取偏移量
    * @param consumerGroupId
    * @param topic
    * @return
    */
  def getOffset(topic: String, consumerGroupId: String): Map[TopicPartition, Long] = {
    val sql=" select group_id,topic,topic_offset,partition_id from offset_0523 " +
      " where topic='"+topic+"' and group_id='"+consumerGroupId+"'"

    val jsonObjList: List[JSONObject] = MySQLUtil.queryList(sql)

    val topicPartitionList: List[(TopicPartition, Long)] = jsonObjList.map {
      jsonObj =>{
        val topicPartition: TopicPartition = new TopicPartition(topic, jsonObj.getIntValue("partition_id"))
        val offset: Long = jsonObj.getLongValue("topic_offset")
        (topicPartition, offset)
      }
    }
    val topicPartitionMap: Map[TopicPartition, Long] = topicPartitionList.toMap
    topicPartitionMap
  }

}
