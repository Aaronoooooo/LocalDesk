package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 	coalesce缩减分区
 * @author zxjgreat
 * @create 2020-06-05 10:17
 */
object TestRDD_Operator5 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD1 = sc.makeRDD(List(1,1,1,2,2,2),6)
//    val dataRDD: RDD[Int] = dataRDD1.filter(_%2==0)
    //当数据过滤后，发现数据不够均匀，那么可以缩减分区
    val coalesceRDD: RDD[Int] = dataRDD1.coalesce(2)
    coalesceRDD.saveAsTextFile("output3")
    sc.stop()
  }

}
