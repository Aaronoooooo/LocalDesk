package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-05 18:05
 */
object TestRDD_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(
        ("a", 1), ("a",2), ("c",3),
        ("b", 4), ("c",5), ("c",6)
      ),2)
    // TODO aggregateByKey ： 根据key进行数据聚合
    // Scala 语法 ： 函数柯里化
    // 方法有两个参数列表需要传递参数
    // 第一个参数列表中传递参数为zeroValue：计算的初始值
    //       用于在分区内进行计算时，当作初始值使用。
    // 第二个参数列表中传递参数为
    //       seqOp ：分区内的计算规则,相同key的value的计算
    //       combOp ：分区间的计算规则,相同key的value的计算
//    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
//      (x, y) => math.max(x, y),
//      (x, y) => x + y
//    )

    // 如果分区内计算规则和分区间的计算规则相同都是求和，那么可以计算wordcount
    val rdd1: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
