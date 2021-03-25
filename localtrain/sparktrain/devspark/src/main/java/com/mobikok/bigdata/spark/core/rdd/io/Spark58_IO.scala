package com.mobikok.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark58_IO {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("input/1.txt")
        rdd.saveAsTextFile("output")

        rdd.saveAsObjectFile("output1")
        val objectRDD: RDD[String] = sc.objectFile[String]("output1")

        rdd.map((_, 1)).saveAsSequenceFile("output2")
        val seqRDD: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output2")

        sc.stop()

    }
}
