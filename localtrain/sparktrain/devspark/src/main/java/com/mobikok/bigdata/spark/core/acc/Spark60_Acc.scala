package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark60_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // 8
        val rdd = sc.makeRDD(List(
            ("a", 1), ("a",2),("a", 3),("a", 4)
        ))

        //val rdd1 = rdd.reduceByKey(_+_)
        //println("sum = " + sum)
        var sum  = 0

        rdd.foreach{
            case ( word, count ) => {
                sum = sum + count
                println(sum)
            }
        }

        println("(a, "+sum+")")



        sc.stop()

    }
}
