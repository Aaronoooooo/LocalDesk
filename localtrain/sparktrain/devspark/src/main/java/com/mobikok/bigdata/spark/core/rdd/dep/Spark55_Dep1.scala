package com.mobikok.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark55_Dep1 {

    def main(args: Array[String]): Unit = {

        // Spark 依赖关系
        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            "hello scala", "hello spark"
        ))
        // TODO OneToOneDependency
        // 依赖关系中，现在的数据分区和依赖前的数据分区一一对应。
        println(rdd.dependencies)
        println("------------------")

        val wordRDD = rdd.flatMap(
            string => {
                string.split(" ")
            }
        )
        println(wordRDD.dependencies)
        println("------------------")

        // TODO OneToOneDependency(1:1)
        val mapRDD = wordRDD.map(
            word => (word, 1)
        )
        println(mapRDD.dependencies)
        println("------------------")

        // TODO ShuffleDependency (N:N)
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey( _ + _ )
        println(reduceRDD.dependencies)
        println("------------------")

        println(reduceRDD.collect().mkString(","))

        sc.stop()

    }
}
