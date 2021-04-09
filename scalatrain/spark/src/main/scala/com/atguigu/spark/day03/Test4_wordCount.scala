package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-04 12:15
 */
object Test4_wordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    //WordCount
    //从文件中获取数据
    val dataRDD: RDD[String] = sc.textFile("input/word.txt")
    //将数据切分
    val wordRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    //将数据分组
    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word=>word)
    //求出wordCount
    val wordCountRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    wordCountRDD.collect().foreach(println)
    sc.stop()
  }
}
