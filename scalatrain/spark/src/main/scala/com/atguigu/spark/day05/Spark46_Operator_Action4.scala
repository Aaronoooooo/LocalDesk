package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-06 20:42
 */
object Spark46_Operator_Action4 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd.map((_,1)).saveAsSequenceFile("output2")

    sc.stop()
  }
}