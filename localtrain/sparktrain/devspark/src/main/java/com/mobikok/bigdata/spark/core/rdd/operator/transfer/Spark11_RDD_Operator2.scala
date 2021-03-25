package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark11_RDD_Operator2 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // 1,2,3,4 => x map 1 => x map 2
        // 1 => x map 1  => x map 2  => 2

        // 0 - (1,2)         1A       1B   2A  2B
        // 1 - (3,4)  3A  3B    4A  4B
        // TODO 分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完毕后才会执行下一条数据
        //      分区间数据执行没有顺序，而且无需等待

        val rdd1: RDD[Int]  = rdd.map( x=>{
            println("map A = " + x)
            x
        })

        val rdd2: RDD[Int]  = rdd1.map( x=>{
            println("map B = " + x)
            x
        })

        println(rdd2.collect().mkString(","))

        sc.stop()
    }
}
