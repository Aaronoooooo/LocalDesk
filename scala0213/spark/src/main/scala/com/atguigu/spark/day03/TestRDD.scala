package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-03 17:28
 */
object TestRDD {
  def main(args: Array[String]): Unit = {
//    val list = List(1,2,3,4)
//    println(list.reduce(_ + _))
    val sparkConf: SparkConf = new SparkConf().setAppName("File - RDD").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
//
//    val rdd: RDD[String] = sc.textFile("input/w.txt",2)
//    rdd.saveAsTextFile("output")
//    sc.stop()
//
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
//    val sc = new SparkContext(sparkConf)

    // TODO hadoop分区是以文件为单位进行划分的。
    //      读取数据不能跨越文件

    // 1@@ => 012
    // 234 => 345

    // 12 / 3 = 4
    // 1.txt => (0, 4)
    //       => (4, 6)
    // 2.txt => (0, 4)
    //       => (4, 6)

    val fileRDD1: RDD[String] = sc.textFile("input", 3)
    fileRDD1.saveAsTextFile("output")
    sc.stop()

  }

}
