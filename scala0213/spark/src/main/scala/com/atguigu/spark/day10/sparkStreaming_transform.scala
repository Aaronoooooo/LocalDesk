package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zxjgreat
 * @create 2020-06-13 10:31
 */
object sparkStreaming_transform {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //执行逻辑
    // TODO 转换
    // Code Driver (1)

    val ds = ssc.socketTextStream("hadoop102",9999)
    val newDS: DStream[String] = ds.transform(
      rdd=>{
        // Code Driver(N)
        rdd.map(
          data => {
            // Code Executor(N)
            data * 2
          }
        )
      }
    )

    // Code : Driver(1)
    val newDS1 = ds.map(
      data => {
        // Code : Executor(N)
        data * 2
      }
    )

    newDS1.print()

    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()


  }

}
