package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * * @create 2020-06-13 10:31
 */
object sparkStreaming_state {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.sparkContext.setCheckpointDir("pc")

    //执行逻辑
    val ds = ssc.socketTextStream("hadoop102",9999)
    // 数据的有状态的保存
    // 将spark每个采集周期数据的处理结果保存起来，然后和后续的数据进行聚合

    // reduceByKey方法是无状态的，而我们需要的有转态的数据操作

    // 有状态的目的其实就是将每一个采集周期数据的计算结果临时保存起来
    // 然后再一次数据的处理中可以继续使用
    ds
        .flatMap(_.split(" "))
        .map((_,1L))

      // updateStateByKey 是有状态计算方法
      // 第一个参数表示 相同key的value的集合
      // 第二个参数表示 想用key的缓冲区的数据,有可能为空
      // requirement failed: The checkpoint directory has not been set.
      // 这里的计算的中间结果需要保存到检查点的位置中,所以需要设定检查点路径
        .updateStateByKey[Long](
          (seq:Seq[Long],buffer:Option[Long])=>{
            val newBuffer = buffer.getOrElse(0L) + seq.sum
            Option(newBuffer)
          }
        ).print()

    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()


  }

}
