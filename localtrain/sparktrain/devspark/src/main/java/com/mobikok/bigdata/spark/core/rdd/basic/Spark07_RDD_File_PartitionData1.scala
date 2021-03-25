package com.mobikok.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark07_RDD_File_PartitionData1 {

    def main(args: Array[String]): Unit = {

        // TODO Scala


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO  Spark - 从磁盘（File）中创建RDD

        // TODO 1. 分几个区？
        //    10 byte / 4 = 2byte...2byte => 5
        //    0 => (0, 2)
        //    1 => (2, 4)
        //    2 => (4, 6)
        //    3 => (6, 8)
        //    4 => (8,10)
        // TODO 2. 数据如何存储?
        //    数据是以行的方式读取，但是会考虑偏移量（数据的offset）的设置
        //    1@@ => 012
        //    2@@ => 345
        //    3@@ => 678
        //    4   => 9


        //    0 => (0, 2) => 1
        //    1 => (2, 4) => 2
        //    2 => (4, 6) => 3
        //    3 => (6, 8) =>
        //    4 => (8,10) => 4

        val fileRDD1: RDD[String] = sc.textFile("input/w.txt", 4)
        fileRDD1.saveAsTextFile("output")
        sc.stop()
    }
}
