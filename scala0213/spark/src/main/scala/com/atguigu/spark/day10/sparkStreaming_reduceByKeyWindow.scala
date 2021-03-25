package com.atguigu.spark.day10

import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zxjgreat
 * @create 2020-06-13 10:31
 */
object sparkStreaming_reduceByKeyWindow {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.sparkContext.setCheckpointDir("pc")

    //执行逻辑
    val ds = ssc.socketTextStream("hadoop102",9999)
    val wordDS: DStream[(String, Int)] = ds.map(num=>("key",num.toInt))
    //一般用于重复数据比较大的情况
    val result: DStream[(String, Int)] = wordDS.reduceByKeyAndWindow(
      (x, y) => {
        x + y
      },
      (a, b) => {
        a - b
      },
      Seconds(9)
    )
    result.foreachRDD(rdd=>rdd.foreach(println))

    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()


  }

}
