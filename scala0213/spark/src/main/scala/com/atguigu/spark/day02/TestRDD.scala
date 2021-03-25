package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-02 20:53
 */
object TestRDD {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    //Spark - 从内存中创建RDD
    // 1. parallelize : 并行
    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4))
    println(rdd.collect().mkString(","))
    //makeRDD,makeRDD的底层代码其实就是调用了parallelize方法
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    rdd1.collect().foreach(println)


    //Spark - 从磁盘（File）中创建RDD
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
    //val fileRDD: RDD[String] = sc.textFile("input/word*.txt")
    // 文件路径还可以指向第三方存储系统：HDFS
    val fileRDD: RDD[String] = sc.textFile("input/word*.txt")

    println(fileRDD.collect().mkString(","))

    //Spark - 从内存中创建RDD
    //    RDD中的分区的数量就是并行度，设定并行度，其实就在设定分区数量
    // 1. makeRDD的第一个参数：数据源
    // 2. makeRDD的第二个参数：默认并行度(分区的数量)
    //                                     parallelize
    //             numSlices: Int = defaultParallelism（默认并行度）

    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 并行度默认会从spark配置信息中获取spark.default.parallelism值。
    // 如果获取不到指定参数，会采用默认值totalCores（机器的总核数）
    // 机器总核数 = 当前环境中可用核数
    // local => 单核（单线程）=> 1
    // local[4] => 4核（4个线程） => 4
    // local[*] => 最大核数 => 8

    val rdd2: RDD[Int] = sc.makeRDD(List(1,2,3,4),3)
    //println(rdd.collect().mkString(","))

    // 将RDD的处理后的数据保存到分区文件中
    rdd2.saveAsTextFile("output")


    sc.stop()

  }

}
