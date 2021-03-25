package com.mobikok.bigdata.spark.streaming.oper

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkStreaming04_File {

    def main(args: Array[String]): Unit = {

        // TODO Spark环境
        // SparkStreaming使用核数最少是2个
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 执行逻辑
        val dirDS: DStream[String] = ssc.textFileStream("in")
        dirDS.print()

        ssc.start()

        // 等待采集器的结束
        ssc.awaitTermination()
    }
}
