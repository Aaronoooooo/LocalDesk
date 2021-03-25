package com.atguigu.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->foldByKey
 * @author zxjgreat
 * @create 2020-06-05 22:00
 */
object wordCount7 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(
        ("hello", 1), ("scala",1), ("spark",1),
        ("spark", 1), ("scala",1), ("scala",1)
      ),2)

    println(rdd.countByKey())
    sc.stop()
  }

}
