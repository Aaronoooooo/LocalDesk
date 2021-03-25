package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark13_RDD_Operator3 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）
        // mapPartitions
        // 以分区为单位进行计算， 和map算子很像
        // 区别就在于map算子是一个一个执行，而mapPartitions一个分区一个分区执行
        // 类似于批处理

        // map方法是全量数据操作，不能丢失数据
        // mapPartitions 一次性获取分区的所有数据，那么可以执行迭代器集合的所有操作
        //               过滤，max,sum

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // 3,4,1,2
//        val rdd: RDD[Int] = dataRDD.mapPartitions(
//            iter => {
//                iter.map(_*2)
//            }
//        )
//        println(rdd.collect.mkString(","))

        val rdd: RDD[Int] = dataRDD.mapPartitions(
            iter => {
                iter.filter(_ % 2 == 0)
            }
        )
        println(rdd.collect.mkString(","))

        sc.stop()
    }
}
