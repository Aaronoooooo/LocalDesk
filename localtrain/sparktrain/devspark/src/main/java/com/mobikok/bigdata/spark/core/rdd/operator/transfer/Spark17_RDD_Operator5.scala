package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark17_RDD_Operator5 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val dataRDD = sc.makeRDD(List(
            List(1,2),List(3,4)
        ))

        val rdd: RDD[Int] = dataRDD.flatMap(list=>list)

        println(rdd.collect().mkString(","))

        sc.stop()
    }
}
