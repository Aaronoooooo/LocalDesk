package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD算子
 *
 * @author zxjgreat
 * @create 2020-06-03 21:24
 */
object TestRDD_Operator {
  // TODO 分区问题
  // RDD中有分区列表
  // 默认分区数量不变，数据会转换后输出
  // TODO 分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完毕后才会执行下一条数据
  //      分区间数据执行没有顺序，而且无需等待

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)
//    val rdd1: RDD[Int] = rdd.map(_*2)
//    rdd1.saveAsTextFile("output")


    // mapPartitions
    // 以分区为单位进行计算， 和map算子很像
    // 区别就在于map算子是一个一个执行，而mapPartitions一个分区一个分区执行
    // 类似于批处理
//    val rdd1: RDD[Int] = rdd.mapPartitions(
//      iter => {
//        iter.map(_ * 2)
//      }
//    )
//    val rdd1: RDD[Int] = rdd.mapPartitions(
//      iter => {
//        iter.filter(
//          _ % 2 == 0
//        )
//      }
//    )
//    val rdd1: RDD[Int] = rdd.mapPartitions(
//      iter => {
//        List(iter.max).iterator
//      }
//    )
    // 获取每个分区最大值以及分区号
//    val rdd1: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
//      (index, iter) => {
//        List((index, iter.max)).iterator
//      }
//    )

    // 获取第二个数据分区的数据
    val rdd1: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    println(rdd1.collect().mkString(","))
    sc.stop()
  }

}
