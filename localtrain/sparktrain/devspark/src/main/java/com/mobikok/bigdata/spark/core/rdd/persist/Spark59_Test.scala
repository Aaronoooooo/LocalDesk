package com.mobikok.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark59_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,1,2))

        val mapRDD: RDD[(Int, Int)] = rdd.map( num=>{
            println("map......")
            (num,1)
        } )

        val reduceRDD = mapRDD.reduceByKey((x,y)=> {
            println("reduce....")
            x + y
        })

        println(reduceRDD.collect().mkString(","))
        println("**********************")
        println(reduceRDD.collect().mkString(","))


        sc.stop()

    }
}
