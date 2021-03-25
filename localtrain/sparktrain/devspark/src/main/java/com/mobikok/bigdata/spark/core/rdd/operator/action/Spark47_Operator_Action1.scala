package com.mobikok.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark47_Operator_Action1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO reduce
        // 简化，规约
        //val i: Int = rdd.reduce(_+_)

        // TODO collect
        // 采集数据
        // collect方法会将所有分区计算的结果拉取到当前节点Driver的内存中，可能会出现内存溢出
        //val array: Array[Int] = rdd.collect()

        // TODO count
        val cnt: Long = rdd.count()

        // TODO first
        val f = rdd.first()

        // TODO take
        val subarray: Array[Int] = rdd.take(3)

        println(cnt)

        sc.stop()
    }
}
