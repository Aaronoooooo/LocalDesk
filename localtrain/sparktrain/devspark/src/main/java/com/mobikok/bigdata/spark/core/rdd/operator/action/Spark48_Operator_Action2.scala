package com.mobikok.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark48_Operator_Action2 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(2,1,4,3))

        // TODO takeOrdered
        // 1,2,3,4 => 1,2,3
        // 2,1,4 => 1,2,4
        val ints: Array[Int] = rdd.takeOrdered(3)
        println(ints.mkString(","))

        sc.stop()
    }
}
