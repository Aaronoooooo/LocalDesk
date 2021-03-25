package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark63_Acc3_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    // TODO 累加器 ： WordCount
    val rdd = sc.makeRDD(List("hello", "hello", "spark", "scala"), 1)

    // 创建累加器
    val acc = new MyWordCountAccumulatorTest

    sc.register(acc)

    rdd.flatMap(_.split(" ")).foreach(
      word => {
        acc.add(word)
      }
    )
    println(acc.value)
  }

  // 自定义累加器
  class MyWordCountAccumulatorTest extends AccumulatorV2[String, mutable.Map[String, Int]] {

    var wordCountMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

    override def isZero: Boolean = {
      wordCountMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyWordCountAccumulatorTest
    }

    override def reset(): Unit = {
      wordCountMap.clear()
    }

    override def add(v: String): Unit = {
      wordCountMap.update(v, wordCountMap.getOrElse(v, 0) + 1)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1: mutable.Map[String, Int] = wordCountMap
      val map2: mutable.Map[String, Int] = other.value
      wordCountMap = map1.foldLeft(map2)(
        (map, kv) => {
          map(kv._1) = map.getOrElse(kv._1, 0) + kv._2

          map
        }
      )
    }

    override def value: mutable.Map[String, Int] = {
      wordCountMap
    }
  }

}
