package com.mobikok.bigdata.spark.streaming.oper

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming13_Continue {

    def main(args: Array[String]): Unit = {


        val ssc = StreamingContext.getActiveOrCreate("cp", getStreamingContext)

        ssc.start()
        ssc.awaitTermination()
    }
    def getStreamingContext() : StreamingContext = {
        // TODO Spark环境
        // SparkStreaming使用核数最少是2个
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        // SparkStreaming中的检查点不仅仅保存中间处理数据，还保存逻辑
        ssc.checkpoint("cp")

        val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        ds.print()

        ssc
    }
}
