package com.atguigu.spark.day05

import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-05 18:05
 */
object TestRDD_cogroup {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(
      List(
        ("a", 1), ("c",3),("b",2)
      ),2)

    val rdd2 = sc.makeRDD(
      List(
        ("a", 4), ("c",6),("b",5)
      ),2)
    rdd1.cogroup(rdd2).collect().foreach(println)
    sc.stop()
  }
}
