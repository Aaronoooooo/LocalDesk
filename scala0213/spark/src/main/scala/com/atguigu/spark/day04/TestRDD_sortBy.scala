package com.atguigu.spark.day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author zxjgreat
 * @create 2020-06-05 18:05
 */
object TestRDD_sortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,4,2,3))
    // TODO sortBy

    // 默认排序规则为 升序
    // sortBy可以通过传递第二个参数改变排序的方式
    // sortBy可以设定第三个参数改变分区。
    val sortRDD: RDD[Int] = rdd.sortBy(num=>num, true,2)

    sortRDD.collect().foreach(println)
    sc.stop()
  }

}
