package com.atguigu.spark.homework

import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->aggregateByKey
 * @author zxjgreat
 * @create 2020-06-05 22:00
 */
object wordCount4 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(
        ("hello", 4), ("scala",1), ("spark",1),
        ("spark", 3), ("scala",1), ("scala",1)
      ),2)

    println(rdd.aggregateByKey(0)(
      (k, v) => k + v,
      (k, v) => k + v
    ).collect().mkString(","))

    sc.stop()
  }

}
