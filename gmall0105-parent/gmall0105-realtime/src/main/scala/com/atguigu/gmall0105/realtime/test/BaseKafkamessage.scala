package com.atguigu.gmall0105.realtime.test

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseKafkamessage {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("customer_service_efficiency").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val topic = "chat-records"
    val groupID = "g1"
//    val kafkaOffsetMap = OffsetManager.getOffset(topic, groupID)
    var recordInputStream: InputDStream[ConsumerRecord[String,String]] = null
//    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
//      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupID)
//    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupID)
//    }

    recordInputStream.print()

    //得到本批次的偏移量的结束位置,用于更新redis中的偏移量
    var  offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val  inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges  //driver? executor?  //周期性的执行
      rdd
    }
    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }

    jsonObjDstream.print()



  }
}
