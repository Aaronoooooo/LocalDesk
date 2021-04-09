package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * * @create 2020-06-05 18:05
 */
object TestRDD_partitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 2)
    ),1
    )
    // TODO Spark中很多的方法是基于Key进行操作，所以数据格式应该为键值对（对偶元组）
    // 如果数据类型为K-V类型，那么Spark会给RDD自动补充很多新的功能（扩展）
    // 隐式转换(A => B)
    // partitionBy方法来自于PairRDDFunctions类
    // RDD的伴生对象中提供了隐式函数可以将RDD[K,V]转换为PairRDDFunction类

    // TODO partitionBy : 根据指定的规则对数据进行分区,默认分区器为HashPartitioner

    val result: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(2))
    result.saveAsTextFile("out")
    sc.stop()
  }

}
