package com.mobikok.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark46_Operator_Action {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 算子 - 行动
        // 所谓的行动算子，其实不会再产生新的RDD，而是触发作业的执行
        // 行动算子执行后，会获取到作业的执行结果。
        // 转换算子不会触发作业的执行，只是功能的扩展和包装。
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // Spark的行动算子执行时，会产生Job对象，然后提交这个Job对象
        val data: Array[Int] = rdd.collect()
        data.foreach(println)

        sc.stop()
    }
}
