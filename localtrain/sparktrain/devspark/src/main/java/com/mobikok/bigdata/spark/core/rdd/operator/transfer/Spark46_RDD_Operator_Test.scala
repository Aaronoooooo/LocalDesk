package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark46_RDD_Operator_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 案例实操

//        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
//        val sc = new SparkContext(sparkConf)
//
//        val rdd: RDD[Int] = sc.makeRDD(List(1,1,2,2),2)
//        // 1->2, 2->2
//        val result = rdd.mapPartitions(
//            iter => {
//                //val len = iter.length
//                //iter.hasNext
//                // 迭代器
//                //List((iter.next(), len)).iterator
//                iter
//            }
//        )
//
//        println(result.collect().mkString(","))
//
//        sc.stop

        val iter: Iterator[Int] = List(1,2,3,4).iterator
        val len: Int = iter.length
        while ( iter.hasNext ) {
            println(iter.next())
        }

    }

}
