package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark28_RDD_Operator10 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,1,2,4))

//        rdd.groupBy(
//            (num:Int)=>num,2
//        )

        // TODO distinct 去重
        val rdd1: RDD[Int] = rdd.distinct()
        // distinct可以改变分区的数量
        val rdd2: RDD[Int] = rdd.distinct(2)

        println(rdd1.collect().mkString(","))
        println(rdd2.collect().mkString(","))

        sc.stop()
    }
}
