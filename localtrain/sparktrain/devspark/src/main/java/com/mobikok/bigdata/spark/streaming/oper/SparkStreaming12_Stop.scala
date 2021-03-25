package com.mobikok.bigdata.spark.streaming.oper

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkStreaming12_Stop {

    def main(args: Array[String]): Unit = {

        // TODO Spark环境
        // SparkStreaming使用核数最少是2个
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val ds = ssc.socketTextStream("localhost", 9999)
        // TODO 窗口
        val wordToOneDS = ds.map(num=>( "key", num ))

        wordToOneDS.print()

        // Stop (X)
        ssc.start()

        // TODO 当业务升级的场合，或逻辑发生变化
        // TODO Stop方法一般不会放置在main线程完成
        // TODO 需要将stop方法使用新的线程完成调用
        new Thread(new Runnable {
            override def run(): Unit = {
                // TODO Stop方法的调用不应该是线程启动后马上调用
                // TODO Stop方法调用的时机，这个时机不容易确定，需要周期性的判断时机出否出现
                while ( true ) {
                    Thread.sleep(10000)
                    // TODO 关闭时机的判断一般不会使用业务操作
                    // TODO 一般采用第三方的程序或存储进行判断
                    // HDFS => /stopSpark
                    // zk
                    // mysql
                    // redis
                    // 优雅地关闭
                    val state: StreamingContextState = ssc.getState()
                    if (state == StreamingContextState.ACTIVE) {
                        ssc.stop(true, true)
                        // TODO SparkStreaming如果停止后，当前的线程也应该同时停止
                        System.exit(0)
                    }

                }
            }
        }).start()
        // Stop (X)
        // 等待采集器的结束
        ssc.awaitTermination()
        // Stop (X)

        //ssc.stop()

        // 线程
//        val t = new Thread()
//        t.start()
//        // i++, long, double
//        t.stop() // 数据安全
    }
}
