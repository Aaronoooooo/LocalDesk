package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-01 16:28
 */
object TestWordCount {

  def main(args: Array[String]): Unit = {

//    //1. 准备Spark环境
//    // setMaster : 设定Spark环境的位置
//    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
//    //2.建立和Spark的连接
//    val sc = new SparkContext(sparkConf)
//    //3.实现业务操作
//    //3.1从文件中读取数据
//    val fileRDD: RDD[String] = sc.textFile("input")
//    //3.2将数据切分
////    val wordRDD: RDD[String] = fileRDD.flatMap(line=>line.split(" "))
//    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
//    //3.3将数据分组
//    val groupRdd: RDD[(String, Iterable[String])] = wordRDD.groupBy(word=>word)
//    //3.4将数据聚合
//    val wordCountRDD: RDD[(String, Int)] = groupRdd.map {
//      case (word, iter) => {
//        (word, iter.size)
//      }
//    }
//    //3.5打印输出
//    val result: Array[(String, Int)] = wordCountRDD.collect()
//    println(result.mkString(","))
//    //4.释放连接
//    sc.stop()

    //1.创建spark环境
//    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
//    //2.建立连接
//    val sc = new SparkContext(sparkConf)
//    //3.业务需求
//    //3.1从文件中读取数据
//    val fileRDD: RDD[String] = sc.textFile("input")
//    //3.2将数据扁平化并切分
//    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
//    //3.3将得到的数据进行结构的转换
//    val wordMapRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))
//    //3.4将转换结构后的数据根据单词进行分组聚合
//    //reduceByKey方法的作用表示根据数据key进行分组，然后对value进行统计聚合
//    val wordCountRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey(_+_)
//    //5.将数据采集打印
//    println(wordCountRDD.collect().mkString(","))
//    //4.释放连接
//    sc.stop()

    //1.创建spark的环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    //2.建立连接
    val sc = new SparkContext(sparkConf)
    //3.业务需求
    //3.1 从文件中获取数据
    val rdd: RDD[String] = sc.textFile("input")
    //3.2将数据扁平化切分
    println(rdd.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).collect().mkString(","))



    //4.关闭连接
    sc.stop()
  }

}
