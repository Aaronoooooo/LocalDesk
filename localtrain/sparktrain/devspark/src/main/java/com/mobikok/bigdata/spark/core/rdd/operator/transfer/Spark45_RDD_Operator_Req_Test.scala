package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark45_RDD_Operator_Req_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("top3")
    val sc = new SparkContext(conf)
    val dataRDD: RDD[String] = sc.textFile("input/agent.log")
    val mapRDD: RDD[(String, Int)] = dataRDD.map(
      data => {
        val datas: Array[String] = data.split(" ")
        (datas(1) + "-" + data(4), 1)
      }

    )
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
      case (adv, count) => {
        val datas: Array[String] = adv.split("-")
        (datas(0), (datas(1), count))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()
    val srotRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    )
    srotRDD.collect().foreach(println)
    sc.stop()

  }
}
