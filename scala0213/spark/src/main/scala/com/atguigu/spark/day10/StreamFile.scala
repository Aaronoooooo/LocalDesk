package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author zxjgreat
 * @create 2020-06-13 10:31
 */
object StreamFile {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //执行逻辑
    val inDS: DStream[String] = ssc.textFileStream("in")
    inDS.print()

    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()


  }

}
