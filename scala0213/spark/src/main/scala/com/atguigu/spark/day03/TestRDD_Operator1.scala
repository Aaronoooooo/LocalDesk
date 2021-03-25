package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-03 22:13
 */
object TestRDD_Operator1 {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)

    //    val dataRDD = sc.makeRDD(List(
    //      List(1,2),List(3,4)
    //    ))
    //
    //    val rdd: RDD[Int] = dataRDD.flatMap(list=>list)
    // 将List(List(1,2),3,List(4,5))进行扁平化操作
//    val dataRDD = sc.makeRDD(
//      List(List(1, 2), 3, List(4, 5))
//    )
//    val rdd: RDD[Any] = dataRDD.flatMap(
//      data => {
//        data match {
//          case list: List[_] => list
//          case d => List(d)
//        }
//      }
//    )

    // glom => 将每个分区的数据转换为数组
    val dataRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    val rdd: RDD[Array[Int]] = dataRDD.glom()
    rdd.foreach(
      array=>{
        println(array.mkString(","))
      }
    )

    sc.stop()
  }

}
