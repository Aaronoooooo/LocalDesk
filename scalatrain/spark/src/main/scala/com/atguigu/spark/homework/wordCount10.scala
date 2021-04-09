package com.atguigu.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordCount-->foldByKey
 * * @create 2020-06-05 22:00
 */
object wordCount10 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List("hello" , "scala" , "spark" , "spark" , "scala" , "scala"))

    val mapRDD: RDD[Map[String, Int]] = rdd.map(word=>Map[String,Int]((word,1)))
    println(mapRDD.reduce(
      (map1, map2) => {
        map1.foldLeft(map2)(
          (map, kv) => {
            val word: String = kv._1
            val count: Int = kv._2
            map.updated(word, map.getOrElse(word, 0) + count)
          }
        )
      }
    ))



    sc.stop()
  }

}
