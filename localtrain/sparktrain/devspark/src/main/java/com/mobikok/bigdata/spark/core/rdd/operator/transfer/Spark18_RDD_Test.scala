package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark18_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // 将List(List(1,2),3,List(4,5))进行扁平化操作
        val dataRDD = sc.makeRDD(
            List(List(1,2),3,List(4,5))
        )

        val rdd = dataRDD.flatMap(
            data => {
                data match {
                    case list: List[_] =>list
                    case d => List(d)
                }
            }
        )

        println(rdd.collect().mkString(","))

        sc.stop()
    }
}
