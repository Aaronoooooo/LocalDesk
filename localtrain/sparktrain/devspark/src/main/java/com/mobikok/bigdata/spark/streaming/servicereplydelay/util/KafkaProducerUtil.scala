package com.mobikok.bigdata.spark.streaming.servicereplydelay.util

import java.io.{BufferedReader, File, FileReader}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
//    props.put("bootstrap.servers", "bigdata1:9092,bigdata2:9092,bigdata3:9092")
    props.put("bootstrap.servers", "104.250.147.226:6667,104.250.150.18:6667,104.250.141.66:6667")
    props.put("acks", "1")
    props.put("batch.size", "16384")
    props.put("linger.ms", "10")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    // 获得log日志
    val srcPath = "C:\\Users\\pro10\\Desktop\\LocalTestDesk\\localtrain\\sparktrain\\devspark\\src\\main\\resources\\chatrecords.txt"

    val srcFile = new File(srcPath)
    val srcReader = new FileReader(srcFile)
    val srcBufferedReader = new BufferedReader(srcReader)

    try {
      var line = srcBufferedReader.readLine()
      var count = 0

      val topic = "chat-records"
      while (line != null) {
        producer.send(
          new ProducerRecord[String, String](topic, Integer.toString(count), line)
        )
        println(s"发送${topic}的${count}条数据>>>" + line)
        line = srcBufferedReader.readLine()
        count += 1
      }
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    } finally {
      srcBufferedReader.close()
      srcReader.close()
      producer.close()
    }
  }
}
