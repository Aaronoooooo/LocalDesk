package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark64_Acc4 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO 累加器 ： WordCount
        val rdd = sc.makeRDD(List("hello", "hello", "spark", "scala"),1)

        // TODO 1. 创建累加器
        val acc = new MyWordCountAccumulator
        // TODO 2. 注册累加器
        sc.register(acc)

        // TODO 3. 使用累加器
        val rdd1 = rdd.map{
            word => {
                acc.add(word)
                word
            }
        }
        println(rdd1.collect().mkString(","))
        rdd1.foreach(println )
        rdd1.foreach(println )
        // TODO 4. 获取累加器的值
        println(acc.value)

        sc.stop()

    }
    // TODO 自定义累加器
    //  1. 继承 AccumulatorV2，定义泛型 [IN, OUT]
    //         IN   : 累加器输入值的类型
    //         OUT : 累加器返回结果的类型
    //  2. 重写方法(6)
    //  3. copyAndReset must return a zero value copy
    class MyWordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

        // 存储WordCount的集合
        var wordCountMap = mutable.Map[String, Int]()

        // TODO 累加器是否初始化
        override def isZero: Boolean = {
            wordCountMap.isEmpty
        }

        // TODO 复制累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
            new MyWordCountAccumulator
        }

        // TODO 重置累加器
        override def reset(): Unit = {
            wordCountMap.clear()
        }

        // TODO 向累加器中增加值
        override def add(word: String): Unit = {

            // word - count
            // word
            //wordCountMap(word) = wordCountMap.getOrElse(word, 0) + 1
            wordCountMap.update(word, wordCountMap.getOrElse(word, 0) + 1)
        }

        // TODO 合并当前累加器和其他累加器
        //      合并累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
            val map1 = wordCountMap
            val map2 = other.value
            wordCountMap = map1.foldLeft(map2)(
                (map, kv) => {
                    map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
                    map
                }
            )
        }

        // TODO 返回累加器的值（Out）
        override def value: mutable.Map[String, Int] = {
            wordCountMap
        }
    }
}
