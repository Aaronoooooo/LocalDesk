package com.atguigu.spark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-05 10:13
 */
object TestRDD_Operator4 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD1 = sc.makeRDD(List(1,2,3,1,2,6))
    val dataRDD2 = sc.makeRDD(List(1,2,3,1,2,6))
    println(dataRDD1.distinct(2).collect().mkString(","))
    println(dataRDD2.distinct().collect().mkString(","))
    sc.stop()
  }

}
