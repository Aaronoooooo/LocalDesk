package com.atguigu.spark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * repartition
 * @author zxjgreat
 * @create 2020-06-05 10:49
 */
object TestRDD_Operator6 {
  def main(args: Array[String]): Unit = {

    //增加分区
    //底层调用coalesce,又shuffle方法
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.makeRDD(List(1,1,1,2,2,2),2)
    dataRDD.repartition(4).saveAsTextFile("output")
    sc.stop()
  }

}
