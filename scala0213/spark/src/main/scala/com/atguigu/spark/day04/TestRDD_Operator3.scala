package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 	sample
 *
 * @author zxjgreat
 * @create 2020-06-05 9:28
 */
object TestRDD_Operator3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.makeRDD(List(1,2,3,4,5,6))
    //
    //false表示抽取不放回，true表示抽取后放回
    val rdd: RDD[Int] = dataRDD.sample(
      false, 0.5
    )
    println(rdd.collect().mkString(","))
    sc.stop()
  }
}
