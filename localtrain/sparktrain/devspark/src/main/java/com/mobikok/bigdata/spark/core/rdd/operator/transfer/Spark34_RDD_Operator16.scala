package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark34_RDD_Operator16 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO K-V类型的数据操作
        // 单值
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("b", 2), ("c", 2)
        ),1
        )

        // TODO Spark中很多的方法是基于Key进行操作，所以数据格式应该为键值对（对偶元组）
        // 如果数据类型为K-V类型，那么Spark会给RDD自动补充很多新的功能（扩展）
        // 隐式转换(A => B)
        // partitionBy方法来自于PairRDDFunctions类
        // RDD的伴生对象中提供了隐式函数可以将RDD[K,V]转换为PairRDDFunction类

        // TODO partitionBy : 根据指定的规则对数据进行分区
        //      groupBy
        //      filter => coalesce
        //      repartition => shuffle
        //
        // partitionBy参数为分区器对象
        //    分区器对象 ： HashPartitioner & RangePartitioner

        // HashPartitioner分区规则是将当前数据的key进行取余操作。
        // TODO HashPartitioner是spark默认的分区器
        val rdd1: RDD[(String, Int)] = rdd.partitionBy( new HashPartitioner(2) )
        //rdd1.saveAsTextFile("output")

        // sortBy 使用了RangePartitioner
        //rdd1.sortBy()

        sc.stop()
    }
}
