package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark16_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // 获取第二个数据分区的数据
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,3,6,2,5,4),3)
        // 获取的分区索引从0开始
        val rdd = dataRDD.mapPartitionsWithIndex(
            (index, iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                    Nil.iterator
                }
            }
        )

        println(rdd.collect().mkString(","))

        sc.stop()
    }
}
