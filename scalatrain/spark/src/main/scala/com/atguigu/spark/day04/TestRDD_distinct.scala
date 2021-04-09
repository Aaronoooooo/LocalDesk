package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-05 17:40
 */
object TestRDD_distinct {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.makeRDD(List(1,2,3,4,1,2))

    //先将数据分组
    val rdd: RDD[(Int, Iterable[Int])] = dataRDD.groupBy(i=>i)
    //取出key
    val result: RDD[Int] = rdd.map(kv=>kv._1)
    result.collect().foreach(println)
    sc.stop()
  }

}
