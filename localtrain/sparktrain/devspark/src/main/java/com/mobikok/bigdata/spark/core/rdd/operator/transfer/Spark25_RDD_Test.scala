package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val dataRDD = sc.makeRDD(List("Hello Scala", "Hello"))

        println(dataRDD
                .flatMap(_.split(" "))
                .groupBy(word => word)
                .map(kv => (kv._1, kv._2.size))
                .collect().mkString(","))


        sc.stop()
    }
}
