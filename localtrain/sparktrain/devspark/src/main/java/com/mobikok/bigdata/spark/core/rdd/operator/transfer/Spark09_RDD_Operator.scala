package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        // 转换算子
        // 能够将旧的RDD通过方法转换为新的RDD，但是不会触发作业的执行

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // 转换：旧RDD => 算子 => 新RDD
//        val rdd1: RDD[Int]  = rdd.map( (i:Int) => { i * 2 } )
//        val rdd1: RDD[Int]  = rdd.map( (i:Int) => i * 2 )
//        val rdd1: RDD[Int]  = rdd.map( (i) => i * 2 )
//        val rdd1: RDD[Int]  = rdd.map( i => i * 2 )
        val rdd1: RDD[Int]  = rdd.map( _ * 2 )

        // 读取数据
        // collect方法不会转换RDD，会触发作业的执行
        // 所以将collectt这样的方法称之为行动（action）算子
        val ints: Array[Int] = rdd1.collect()

        sc.stop()
    }
}
