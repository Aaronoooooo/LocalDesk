package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * * @create 2020-06-05 18:05
 */
object TestRDD_partitionBy1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    // TODO 自定义分区器 - 自己决定数据放置在哪个分区做处理
    // cba, wnba, nba
    val rdd = sc.makeRDD(
      List(
        ("cba", "消息1"),("cba", "消息2"),("cba", "消息3"),
        ("nba", "消息4"),("wnba", "消息5"),("nba", "消息6")
      ),
      1
    )
    val rdd1: RDD[(String, String)] = rdd.partitionBy(new HashPartitioner(3))
    val rdd2: RDD[(Int, (String, String))] = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          data => {
            (index, data)
          }
        )
      }
    )
    rdd2.collect().foreach(println)
    sc.stop()
  }

  class MyPartitioner(i : Int) extends Partitioner{
    override def numPartitions: Int = {i}

    override def getPartition(key: Any): Int = {
      key match{
        case "cba" => 0
        case "nba" => 1
        case "wnba" => 2
      }
    }
  }
}
