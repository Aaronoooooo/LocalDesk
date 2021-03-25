package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // glom
        // 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        //
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)

        // 将每个分区的数据转换为数组
        val glomRDD: RDD[Array[Int]] = dataRDD.glom()

        // 将数组中的最大值取出
        // Array => max
        val maxRDD: RDD[Int] = glomRDD.map(array=>array.max)

        // 将取出的最大值求和
        val array: Array[Int] = maxRDD.collect()

        println(array.sum)

        sc.stop()
    }
}
