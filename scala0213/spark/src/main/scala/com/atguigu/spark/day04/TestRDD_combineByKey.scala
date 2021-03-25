package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-05 18:05
 */
object TestRDD_combineByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    // TODO combineByKey

    // TODO 每个key的平均值 : 相同key的总和 / 相同key的数量

    // 0 =>【("a", 88), ("b", 95), ("a", 91)】
    // 1 =>【("b", 93), ("a", 95), ("b", 98)】
    val rdd = sc.makeRDD(
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    // TODO combineByKey方法可以传递3个参数
    //  第一个参数表示的就是将计算的第一个值转换结构
    //  第二个参数表示分区内的计算规则
    //  第三个参数表示分区间的计算规则
    val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey(
     v=>(v,1),
      (t:(Int,Int),v)=>{
        (t._1 + v,t._2 + 1)
      },
      (t1:(Int,Int),t2:(Int,Int))=>{
        (t1._1+t2._1,t1._2+t2._2)
      }

    )
    val rdd2: RDD[(String, Int)] = rdd1.map {
      case (key, (total, count)) => {
        (key, total / count)
      }
    }
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
