package com.mobikok.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark58_Persist3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)
        sc.setCheckpointDir("cp")

        val rdd = sc.makeRDD(List(1,2,3,4))

        val mapRDD: RDD[(Int, Int)] = rdd.map( num=>{
            //println("map......")
            (num,1)
        } )

        // TODO 检查点操作会切断血缘关系。一旦数据丢失不会重头读取数据
        //      因为检查点会将数据保存到分布式存储系统中，数据相对来说比较安全。不容易丢失。
        //      所以会切断血缘，等同于产生新的数据源。
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)
        println(mapRDD.collect().mkString(","))
        println(mapRDD.toDebugString)


        sc.stop()

    }
}
