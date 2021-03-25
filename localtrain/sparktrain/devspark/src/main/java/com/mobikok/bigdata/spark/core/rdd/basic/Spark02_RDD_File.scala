package com.mobikok.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO  Spark - 从磁盘（File）中创建RDD
        // path : 读取文件（目录）的路径
        // path可以设定相对路径，如果是IDEA，那么相对路径的位置从项目的根开始查找。
        // path路径根据环境的不同自动发生改变。

        // Spark读取文件时，默认采用的是Hadoop读取文件的规则
        // 默认是一行一行的读取文件内容

        // 如果路径指向的为文件目录。那么这个目录中的文本文件都会被读取
        //val fileRDD: RDD[String] = sc.textFile("input")
        // 读取指定的文件
        //val fileRDD: RDD[String] = sc.textFile("input/word.txt")
        // 文件路径可以采用通配符
        val fileRDD: RDD[String] = sc.textFile("input/word*.txt")
        // 文件路径还可以指向第三方存储系统：HDFS
        //val fileRDD: RDD[String] = sc.textFile("input/word*.txt")

        println(fileRDD.collect().mkString(","))



        sc.stop()
    }
}
