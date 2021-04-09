package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-06 20:42
 */
object Spark46_Operator_Action2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(2,1,4,3))

    // TODO takeOrdered
    // 1,2,3,4 => 1,2,3
    // 2,1,4 => 1,2,4
    val ints: Array[Int] = rdd.takeOrdered(3)
    println(ints.mkString(","))

    sc.stop()
  }
}