package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark37_RDD_Operator19 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）
        // List(
        //     ("hello", 4), ("hello, scala", 3)
        // )

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

       // TODO reduceByKey : 根据数据的key进行分组，然后对value进行聚合
        val rdd = sc.makeRDD(
            List(
                ("hello", 1), ("scala",1), ("hello",1)
            )
        )

        // word => (word, 1)
        // reduceByKey 第一个参数表示相同key的value的聚合方式
        // reduceByKey 第二个参数表示聚合后的分区数量
        val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_+_)
        val rdd2: RDD[(String, Int)] = rdd.reduceByKey(_+_,2)

        println(rdd1.collect().mkString(","))

        sc.stop
    }
}
