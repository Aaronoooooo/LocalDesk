package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-04 10:14
 */
object Test1_sum {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.makeRDD(List(1,2,3,4),2)

    //小功能：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val arrayRDD: RDD[Array[Int]] = dataRDD.glom()
    //取出每个组的最大值
    val rdd: RDD[Int] = arrayRDD.map(
      array => {
        array.max
      }
    )
    println(rdd.collect().sum)
    sc.stop()
  }

}
