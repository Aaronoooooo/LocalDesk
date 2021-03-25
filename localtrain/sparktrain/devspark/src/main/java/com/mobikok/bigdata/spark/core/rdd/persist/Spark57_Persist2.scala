package com.mobikok.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark57_Persist2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)
        sc.setCheckpointDir("cp")

        val rdd = sc.makeRDD(List(1,2,3,4))

        val mapRDD: RDD[(Int, Int)] = rdd.map( num=>{
            println("map......")
            (num,1)
        } )

        // TODO 将比较耗时，比较重要的数据一般会保存到分布式文件系统中。
        //      使用checkpoint方法将数据保存到文件中
        //      SparkException: Checkpoint directory has not been set in the SparkContext
        //      执行checkpoint方法前应该设定检查点的保存目录
        //      检查点的操作中为了保证数据的准确性，会执行时，会启动新的job
        //      为了提高性能，检查点操作一般会和cache联合使用
        val cacheRDD: RDD[(Int, Int)] = mapRDD.cache()
        cacheRDD.checkpoint()
        println(cacheRDD.collect().mkString(","))
        println("***************************")
        println(cacheRDD.collect().mkString("&"))
        println(cacheRDD.collect().mkString("*"))


        sc.stop()

    }
}
