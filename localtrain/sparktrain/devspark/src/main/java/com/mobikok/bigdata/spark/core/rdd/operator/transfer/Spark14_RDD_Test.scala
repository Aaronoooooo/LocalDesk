package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark14_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,3,6,2,5,4),2)
        // 获取每个数据分区的最大值
        val rdd: RDD[Int] = dataRDD.mapPartitions(
            iter => {
                List(iter.max).iterator
            }
        )
        println(rdd.collect().mkString(","))

        sc.stop()
    }
}
