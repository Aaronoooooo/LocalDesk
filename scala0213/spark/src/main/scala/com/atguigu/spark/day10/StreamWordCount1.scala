package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zxjgreat
 * @create 2020-06-13 10:31
 */
object StreamWordCount1 {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //执行逻辑
    //从socket获取数据是以行为单位
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))
    val wordToSum: DStream[(String, Int)] = wordDS.map((_,1)).reduceByKey(_+_)
    wordToSum.print()
    //关闭
//    ssc.stop()

    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()


  }

}
