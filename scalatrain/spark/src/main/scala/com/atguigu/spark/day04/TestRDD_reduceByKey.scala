package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * * @create 2020-06-05 18:05
 */
object TestRDD_reduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(
        ("hello", 1), ("scala",1), ("hello",1)
      )
    )

    sc.defaultParallelism
    // word => (word, 1)
    // reduceByKey 第一个参数表示相同key的value的聚合方式
    // reduceByKey 第二个参数表示聚合后的分区数量
    rdd.reduceByKey(_+_).collect().foreach(println)
    sc.stop()


  }
}
