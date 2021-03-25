package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author zxjgreat
 * @create 2020-06-13 10:31
 */
object StreamQueue {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))




    //执行逻辑
    val queue = new mutable.Queue[RDD[String]]()

    val queueDS: InputDStream[String] = ssc.queueStream(queue)
    queueDS.print()

    //启动采集器
    ssc.start()
    for(i <- 1 to 5){
      val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString))
      queue.enqueue(rdd)
      Thread.sleep(1000)

    }

    //等待采集器的结束
    ssc.awaitTermination()


  }

}
