package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-05 18:05
 */
object TestRDD_sortByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(
        ("a", 1), ("c",3),("b",2)
      ),2)

    val rdd1: RDD[(String, Int)] = rdd.sortByKey(true)
    val rdd2: RDD[(String, Int)] = rdd.sortByKey(false)
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}
