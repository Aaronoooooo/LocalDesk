package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark31_RDD_Operator13 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO coalesce
        val rdd: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2), 3)

        // TODO 缩减分区 : coalesce
        rdd.coalesce(2)
        // TODO 扩大分区 ： repartition
        // 从底层源码的角度。repartition其实就是coalesce，并且肯定进行shuffle操作
        rdd.repartition(6)

        sc.stop()
    }
}
