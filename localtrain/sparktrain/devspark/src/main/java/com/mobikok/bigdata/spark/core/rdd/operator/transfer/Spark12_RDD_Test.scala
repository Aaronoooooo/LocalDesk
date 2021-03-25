package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark12_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 从服务器日志数据apache.log中获取用户请求URL资源路径
        val fileRDD = sc.textFile("input/apache.log")

        val urlRDD: RDD[String] = fileRDD.map(
            line => {
                val datas: Array[String] = line.split(" ")
                datas(6)
            }
        )
        urlRDD.collect().foreach(println)


        sc.stop()
    }
}
