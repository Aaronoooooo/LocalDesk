package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-05 18:05
 */
object TestRDD_dubleValues {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6),2)
    val rdd0: RDD[String] = sc.makeRDD(List("a","b","c","d"),2)
    // TODO 并集, 数据合并，分区也会合并
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    println(unionRDD.collect().mkString(","))
    // TODO 交集 ： 保留最大分区数 ，数据被打乱重组, shuffle
    val interRDD: RDD[Int] = rdd1.intersection(rdd2)
    println(interRDD.collect().mkString(","))
    // TODO 差集 : 数据被打乱重组, shuffle
    // 当调用rdd的subtract方法时，以当前rdd的分区为主，所以分区数量等于当前rdd的分区数量
    val subRDD: RDD[Int] = rdd1.subtract(rdd2)
    println(subRDD.collect().mkString(","))
    // TODO 拉链 : 分区数不变

    // TODO 2个RDD的分区一致,但是数据量不相同的场合:
    //   Exception: Can only zip RDDs with same number of elements in each partition
    // TODO 2个RDD的分区不一致，数据量也不相同，但是每个分区数据量一致：
    //   Exception：Can't zip RDDs with unequal numbers of partitions: List(3, 2)
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    //println(rdd6.collect().mkString(","))
    rdd6.saveAsTextFile("output6")






    sc.stop()
  }

}
