package com.mobikok.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark54_Dep {

    def main(args: Array[String]): Unit = {

        // Spark 依赖关系
        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        // TODO : new ParallelCollectionRDD
        val rdd = sc.makeRDD(List(
            "hello scala", "hello spark"
        ))
        println(rdd.toDebugString)
        println("------------------")

        // TODO : new MapPartitionsRDD -> new ParallelCollectionRDD
        val wordRDD = rdd.flatMap(
            string => {
                string.split(" ")
            }
        )
        println(wordRDD.toDebugString)
        println("------------------")

        // TODO : new MapPartitionsRDD -> new MapPartitionsRDD
        val mapRDD = wordRDD.map(
            word => (word, 1)
        )
        println(mapRDD.toDebugString)
        println("------------------")

        // TODO : new ShuffledRDD -> new MapPartitionsRDD
        // Error
        // 如果Spark的计算过程中某一个节点计算失败，那么框架会尝试重新计算
        // Spark既然想要重新计算，那么就需要知道数据的来源，并且还要知道数据经历了哪些计算
        // RDD不保存计算的数据，但是会保存元数据信息
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey( _ + _ )
        println(reduceRDD.toDebugString)
        println("------------------")

        println(reduceRDD.collect().mkString(","))

        sc.stop()

    }
}
