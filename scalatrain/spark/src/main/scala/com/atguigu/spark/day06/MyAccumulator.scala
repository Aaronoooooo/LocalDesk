package com.atguigu.spark.day06

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * * @create 2020-06-08 14:59
 */
object MyAccumulator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello scala" , "hello" , "spark"))

    //1.创建累加器
    val acc = new MyAccumulator
    //2.注册累加器
    sc.register(acc)
    //3.使用累加器
    rdd.flatMap(_.split(" ")).foreach{
      word=>{
        acc.add(word)
      }
    }
    //4.获取累加器的值
    println(acc.value)
    sc.stop()
  }

  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
    var wordCountMap = mutable.Map[String,Int]()
    //是否初始化
    override def isZero: Boolean = {
      wordCountMap.isEmpty
    }

    //复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyAccumulator
    }

    //重置累加器
    override def reset(): Unit = {
      wordCountMap.clear()
    }

    //向累加器中增加数据
    override def add(word: String): Unit = {
      wordCountMap.update(word,wordCountMap.getOrElse(word,0) + 1)
    }

    //合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = wordCountMap
      val map2 = other.value
      wordCountMap = map1.foldLeft(map2)(
        (map, kv) => {
//          map.update(kv._1, map.getOrElse(kv._1, 0) + kv._2)
//          //          map(kv._1)=map.getOrElse(kv._1,0)+kv._2
//          map
          val word = kv._1
          val count = kv._2
          map.update(word,map.getOrElse(word,0) + count)
          map
        }
      )
      wordCountMap
    }

    //返回累加器的结果
    override def value: mutable.Map[String, Int] = {
      wordCountMap
    }
  }
}
