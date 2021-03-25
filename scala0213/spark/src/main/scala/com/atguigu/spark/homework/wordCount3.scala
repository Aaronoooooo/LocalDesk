package com.atguigu.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->groupByKey
 * @author zxjgreat
 * @create 2020-06-05 22:00
 */
object wordCount3 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(
        ("hello", 4), ("scala",1), ("hello",1)
      )
    )
    println(rdd.groupByKey().map {
      case (word, iter) => {
        (word, iter.sum)
      }
    }.collect().mkString(","))
    sc.stop()
  }

}
