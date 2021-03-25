package com.mobikok.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark49_Operator_Action3 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // TODO sum
        //val d: Double = rdd.sum()
        //println(d)
        // TODO aggregate
        // aggregateByKey ： 初始值只参与到分区内计算
        // aggregate ： 初始值分区内计算会参与，同时分区间计算也会参与
        //val i: Int = rdd.aggregate(10)(_+_,_+_)
        //println(i)

        // TODO fold
        //val i: Int = rdd.fold(10)(_+_)
        //println(i)

        // TODO countByKey - 7
//        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
//            ("a",4),("a",1),("a",1)
//        ))
//        val wordToCount: collection.Map[String, Long] = rdd.countByKey()
//
//        println(wordToCount)

        // TODO countByValue - 8
        val rdd: RDD[String] = sc.makeRDD(List(
            "a","a","a","hello", "hello"
        ))

        val wordToCount: collection.Map[String, Long] = rdd.countByValue()
        println(wordToCount)



        sc.stop()
    }
}
