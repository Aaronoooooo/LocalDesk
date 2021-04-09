package com.atguigu.spark.day06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-08 21:19
 */
object TestBroadcast {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(("a",1),("b",2), ("c",3)))
    val list = List(("a",4),("b",5), ("c",6))
    //声明广播变量
    val bcList: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    //使用广播变量
//    val mapRDD: RDD[(String, (Int, Int))] = rdd1.map {
//      case (word, count1) => {
//        var count2 = 0
//        for (kv <- bcList.value) {
//          val k = kv._1
//          val v = kv._2
//          if (k == word) {
//            count2 = v
//          }
//        }
//        (word, (count1, count2))
//      }
//    }
//    println(mapRDD.collect().mkString(","))
    println(rdd1.map {
      case (word, count1) => {
        var count2 = 0
        for (kv <- bcList.value) {
          val k: String = kv._1
          val v: Int = kv._2
          if (k == word) {
            count2 = v
          }
        }
        (word, (count1, count2))
      }
    }.collect().mkString(","))
    sc.stop()
  }
}
