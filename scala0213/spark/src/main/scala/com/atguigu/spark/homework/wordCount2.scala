package com.atguigu.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->reduceByKey
 * @author zxjgreat
 * @create 2020-06-05 22:00
 */
object wordCount2 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.textFile("input/word.txt")
    println(dataRDD.flatMap(_.split(" ")).map(word => (word, 1)).
      reduceByKey(_ + _).collect().mkString(","))
    sc.stop()
  }

}
