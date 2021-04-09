package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * * @create 2020-06-13 10:31
 */
object sparkStreaming_stop {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //执行逻辑
    val ds = ssc.socketTextStream("hadoop102",9999)
    val wordDS: DStream[(String, String)] = ds.map(num=>("key",num))
    //一般用于重复数据比较大的情况
    wordDS.print()

    //启动采集器
    ssc.start()

    // TODO 当业务升级的场合，或逻辑发生变化
    // TODO Stop方法一般不会放置在main线程完成
    // TODO 需要将stop方法使用新的线程完成调用
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true){
          Thread.sleep(10000)
          // TODO 关闭时机的判断一般不会使用业务操作
          // TODO 一般采用第三方的程序或存储进行判断
          // HDFS => /stopSpark
          // zk
          // mysql
          // redis
          // 优雅地关闭
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE){
            ssc.stop(true,true)
            // TODO SparkStreaming如果停止后，当前的线程也应该同时停止
            System.exit(0)
          }
        }
      }
    }).start()
    //等待采集器的结束
    ssc.awaitTermination()



  }

}
