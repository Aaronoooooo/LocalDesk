package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-05 18:05
 */
object TestRDD_exersize {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    //1.从文件读取数据
    val dataRDD: RDD[String] = sc.textFile("input/agent.log")
    //2.将数据进行结构的转换
    //（（省份-广告），1）
    val mapRDD: RDD[(String, Int)] = dataRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        ((datas(1) + "-" + datas(4)), 1)
      }
    )
    //3.将数据进行聚合
    //（（省份-广告），sum）
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    //4.将聚合后的数据进行转换
    //(省份，(广告，sum))
    val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, sum) =>{
        val ks: Array[String] = key.split("-")
        (ks(0),(ks(1),sum))
      }

    }
    //5.分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()
    //6.排序，取前三
    groupRDD.mapValues(
      iter => iter.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(3)
    ).collect().foreach(println)










    sc.stop()
  }
}
