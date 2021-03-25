package com.mobikok.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark52_Serial {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 序列化
//        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//        rdd.foreach(
//            num => {
//                val user = new User()
//                println( "age = " + (user.age + num) )
//            }
//        )

        // TODO Exception : Task not serializable
        // 如果算子中使用了算子外的对象，那么在执行时，需要保证这个对象能序列化。
//        val user = new User()
//        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//
//        rdd.foreach(
//            num => {
//                println( "age = " + (user.age + num) )
//            }
//        )

        // TODO Scala 闭包
        val user = new User()
        val rdd: RDD[Int] = sc.makeRDD(List())

        // Spark的算子的操作其实都是闭包，所以闭包有可能包含外部的变量
        // 如果包含外部的变量，那么就一定要保证这个外部变量可序列化。
        // 所以Spark在提交作业之前，应该对闭包内的变量进行检测，检测是否能够序列化
        // 将这个操作为闭包检测
        rdd.foreach(
            num => {
                println( "age = " + (user.age + num) )
            }
        )

        sc.stop()
    }
//    class User extends Serializable {
//        val age:Int = 20
//    }
    class User {
        val age:Int = 20
    }
    // 样例类自动混入可序列化特质
//    case class User(age:Int = 20)
}
