package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aaron
  * @date 2020/07/27
  */
object Spark61_Acc1Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,3,4,7))
    val sum: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach(
    num => {
      sum.add(num)
    }
    )
    println("结果为 = " + sum.value)
  }

}
