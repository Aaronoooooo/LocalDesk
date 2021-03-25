package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author zxjgreat
 * @create 2020-06-01 19:16
 */
object TestWordCount1 {
  def main(args: Array[String]): Unit = {
    //    //1.创建spark环境
    //    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    //    //2.建立spark连接
    //    val sc = new SparkContext(sparkConf)
    //    //3.业务需求
    //    //3.1从文件中读取数据
    //    val fileRDD: RDD[String] = sc.textFile("input")
    //    //3.2将数据切分
    //    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    //    //3.3将数据分组
    //    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word=>word)
    //    //3.4将数据聚合
    //    val mapRDD: RDD[(String, Int)] = groupRDD.map {
    //      case (word, iter) => {
    //        (word, iter.size)
    //      }
    //    }
    //    //3.5将数据采集后打印
    //    val wordCountArray: Array[(String, Int)] = mapRDD.collect()
    //    println(wordCountArray.mkString(","))
    //
    //    //4.释放连接
    //    sc.stop()
    //
    //    //1.创建spark环境
    //    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    //    //2.建立连接
    //    val sc = new SparkContext(sparkConf)
    //    //3.业务需求
    //    //3.1获取数据
    //    val fileRDD: RDD[String] = sc.textFile("input")
    //    //3.2将数据扁平化后切割
    //    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    //    //3.3将扁平化后的数据进行转换
    //    val wordMapRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))
    //    //3.4将转换后的数据聚合
    //    val WordCountMapRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey(_+_)
    //    //3.5采集数据打印
    //    println(WordCountMapRDD.collect().mkString(","))
    //    //4.释放连接
    //    sc.stop()

    //1.创建spark环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    //2.连接spark
    val sc = new SparkContext(sparkConf)
    //3.业务逻辑
    //3.1从文件获取数据
    val fileRDD: RDD[String] = sc.textFile("input")
    //3.2将数据扁平化，切分
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    //3.3将数据转换
    val wordMapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
    //3.4将数据聚合
    val wordCountMapRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey(_ + _)
    //3.5采集数据并打印
    println(wordCountMapRDD.collect().mkString(","))
    //4.释放连接
    sc.stop()
  }

}
