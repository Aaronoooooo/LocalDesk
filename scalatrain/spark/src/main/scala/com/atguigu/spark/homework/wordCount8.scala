package com.atguigu.spark.homework

import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->foldByKey
 * * @create 2020-06-05 22:00
 */
object wordCount8 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List("hello" , "scala" , "spark" , "spark" , "scala" , "scala"))

    println(rdd.countByValue())

    sc.stop()
  }

}
