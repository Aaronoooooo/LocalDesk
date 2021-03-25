package com.mobikok.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark51_Operator_Action5 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO foreach 方法
        // 集合的方法中的代码是在当前节点(Driver)中执行的。
        //foreach方法是在当前节点的内存中完成数据的循环
        rdd.collect().foreach(println)
        println("*******************************")
        // TODO foreach 算子
        // rdd的方法称之为算子
        // 算子的逻辑代码是在分布式计算节点Executor执行的
        // foreach算子可以将循环在不同的计算节点完成
        // 算子之外的代码是在Driver端执行
        rdd.foreach(println)

        sc.stop()
    }
}
