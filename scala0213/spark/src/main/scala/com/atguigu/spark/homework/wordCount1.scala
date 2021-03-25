package com.atguigu.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->groupBy
 * @author zxjgreat
 * @create 2020-06-05 22:00
 */
object wordCount1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.textFile("input/word.txt")
    println(dataRDD.flatMap(_.split(" ")).groupBy(word => word).map(kv=>{(kv._1,kv._2.size)})
    .collect().mkString(","))


    sc.stop()
  }

}
