package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-04 11:10
 */
object Test2_groupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    //将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组
    val dataRDD: RDD[String] = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"))
    val rdd: RDD[(Char, Iterable[String])] = dataRDD.groupBy(
      word => {
        word.charAt(0)
      }
    )
    println(rdd.collect().mkString(","))
    sc.stop()
  }

}
