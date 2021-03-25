package com.mobikok.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        // TODO  Spark - 从内存中创建RDD
        // 1. parallelize : 并行
        val list = List(1,2,3,4)
        val rdd: RDD[Int] = sc.parallelize(list)
        println(rdd.collect().mkString(","))

        // TODO makeRDD的底层代码其实就是调用了parallelize方法
        val rdd1: RDD[Int] = sc.makeRDD(list)
        println(rdd1.collect().mkString(","))

        sc.stop()
    }
}
