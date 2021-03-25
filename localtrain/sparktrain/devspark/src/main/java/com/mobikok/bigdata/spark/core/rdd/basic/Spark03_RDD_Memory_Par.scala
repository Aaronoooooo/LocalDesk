package com.mobikok.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark03_RDD_Memory_Par {

    def main(args: Array[String]): Unit = {

        // TODO : Scala
        //   1.  参数默认值

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        // TODO  Spark - 从内存中创建RDD
        //    RDD中的分区的数量就是并行度，设定并行度，其实就在设定分区数量
        // 1. makeRDD的第一个参数：数据源
        // 2. makeRDD的第二个参数：默认并行度(分区的数量)
        //                                     parallelize
        //             numSlices: Int = defaultParallelism（默认并行度）

        // scheduler.conf.getInt("spark.default.parallelism", totalCores)
        // 并行度默认会从spark配置信息中获取spark.default.parallelism值。
        // 如果获取不到指定参数，会采用默认值totalCores（机器的总核数）
        // 机器总核数 = 当前环境中可用核数
        // local => 单核（单线程）=> 1
        // local[4] => 4核（4个线程） => 4
        // local[*] => 最大核数 => 8

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),3)
        //println(rdd.collect().mkString(","))

        // 将RDD的处理后的数据保存到分区文件中
        rdd.saveAsTextFile("output")


        sc.stop()
    }
}
