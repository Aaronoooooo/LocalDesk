package com.mobikok.bigdata.spark.streaming.oper

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata1", 9999)
    val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))
    val mapDS: DStream[(String, Int)] = wordDS.map((_, 1))
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)
    reduceDS.print()
    ssc.start()
    Thread.sleep(2000)
    ssc.awaitTermination()
  }
}
