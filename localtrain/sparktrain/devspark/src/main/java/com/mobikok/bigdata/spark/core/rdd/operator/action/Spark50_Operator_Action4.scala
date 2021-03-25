package com.mobikok.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark50_Operator_Action4 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        rdd.map((_,1)).saveAsSequenceFile("output2")

        sc.stop()
    }
}
