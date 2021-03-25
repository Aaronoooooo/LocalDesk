package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark42_RDD_Operator24 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

//        val rdd = sc.makeRDD(
//            List(
//                ("a",1),("c", 3),("b",2)
//            )
//        )
//        val rdd = sc.makeRDD(
//            List(
//                ( new User(), 1 ),
//                ( new User(), 2 ),
//                ( new User(), 3 ),
//            )
//        )
//        val sortRDD = rdd.sortByKey()
//
//        println(sortRDD.collect().mkString(","))

        sc.stop
    }
    // 如果自定义key进行排序，需要将key混入特质Ordered
    //class User extends Ordered[User] with Serializable {
    //class User extends Serializable {
//        override def compare(that: User): Int = {
//            1
//        }
    //}
}
