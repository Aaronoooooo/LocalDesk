package com.atguigu.spark.homework

import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->foldByKey
 * * @create 2020-06-05 22:00
 */
object wordCount6 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(
        ("hello", 4), ("scala",1), ("spark",1),
        ("spark", 3), ("scala",1), ("scala",1)
      ),2)

    println(rdd.combineByKey(
      v => v,
      (i: Int, v) => {
        (i + v)
      },
      (i1: Int, i2: Int) => {
        (i1 + i2)
      }
    ).collect().mkString(","))
    sc.stop()
  }

}
