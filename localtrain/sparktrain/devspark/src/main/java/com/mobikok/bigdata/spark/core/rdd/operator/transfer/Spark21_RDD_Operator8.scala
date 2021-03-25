package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark21_RDD_Operator8 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val dataRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

        // TODO 过滤
        // 根据指定的规则对数据进行筛选过滤，满足条件的数据保留，不满足的数据丢弃
        val rdd: RDD[Int] = dataRDD.filter(
            num => {
                num % 2 == 0
            }
        )

        rdd.collect().foreach(println)




        sc.stop()
    }
}
