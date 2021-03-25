package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Test {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // 根据单词首写字母进行分组
        val dataRDD = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"), 2)

        dataRDD.groupBy(word=>{
//            word.substring(0,1)
//            word.charAt(0)

            //String(0) => StringOps
            // 隐式转换
            word(0)
        })


        sc.stop()
    }
}
