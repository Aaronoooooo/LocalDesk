package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zxjgreat
 * @create 2020-06-13 10:31
 */
object sparkStreaming_window {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.sparkContext.setCheckpointDir("pc")

    //执行逻辑
    val ds = ssc.socketTextStream("hadoop102",9999)
    val wordToOneDS: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1))
    // TODO 将多个采集周期作为计算的整体
    // 窗口的范围应该是采集周期的整数倍
    // 默认滑动的幅度（步长）为一个采集周期
//    val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(9))
    // 窗口的计算的周期等同于窗口的滑动的步长。
    // 窗口的范围大小和滑动的步长应该都是采集周期的整数倍
    wordToOneDS.window(Seconds(9),Seconds(6)).print()

    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()


  }

}
