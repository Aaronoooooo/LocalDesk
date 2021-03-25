package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val fileRDD: RDD[String] = sc.textFile("input/apache.log")

        val timeRDD = fileRDD.map(
            log => {
                val datas = log.split(" ")
                datas(3)
            }
        )

        val hourRDD: RDD[(String, Iterable[String])] = timeRDD.groupBy(
            time => {
                time.substring(11, 13)
            }
        )


        sc.stop()
    }
}
