package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark27_RDD_Operator9 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

        // TODO sample用于从数据集中抽取数据
        // 第一个参数表示数据抽取后是否放回，可以重复抽取
        //    true : 抽取后放回
        //    false: 抽取不放回
        // 第二个参数表示数据抽取的几率（不放回的场合）， 重复抽取的次数（放回的场合）
        //    这里的几率不是数据能够被抽取的数据总量的比率。
        // 第三个参数表示随机数的种子,可以确定数据的抽取
        //    随机数不随机，所谓的随机数依靠随机算法实现
        val dataRDD: RDD[Int] = rdd.sample(
            false,
            0.5,
            1
        )
//        val dataRDD1: RDD[Int] = rdd.sample(
//            true,
//            2
//        )
        println(dataRDD.collect().mkString(","))
        //println(dataRDD1.collect().mkString(","))



        sc.stop()
    }
}
