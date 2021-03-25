package com.mobikok.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark55_Persist {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        sparkConf.set("spark.local.dir.blockmgr", "D:\\mineworkspace\\idea\\classes\\classes-0213\\input")
        val sc = new SparkContext(sparkConf)


        val rdd = sc.makeRDD(List(1,2,3,4))
        rdd.map((_,1)).groupByKey()

        val mapRDD: RDD[(Int, Int)] = rdd.map( num=>{
            println("map......")
            (num,1)
        } )

        // 将计算结果进行缓存，重复使用，提高效率
        // 默认的缓存是存储在Executor端的内存中,数据量大的时候，该如何处理？
        // TODO 缓存cache底层其实调用的persist方法
        // persist方法在持久化数据时会采用不同的存储级别对数据进行持久化操作
        // cache缓存的默认操作就是将数据保存到内存
        // cache存储的数据在内存中，如果内存不够用，executor可以将内存的数据进行整理，然后可以丢弃数据。
        // 如果由于executor端整理内存导致缓存的数据丢失，那么数据操作依然要重头执行。
        // 如果cache后的数据重头执行数据操作的话，那么必须要遵循血缘关系，所以cache操作不能删除血缘关系。
        val cacheRDD: RDD[(Int, Int)] = mapRDD.cache()
        // TODO collect
        println(cacheRDD.collect().mkString(","))
        println("****************************")
        // TODO Save
        println(cacheRDD.collect().mkString("&"))
        //cacheRDD.saveAsTextFile("output")

        sc.stop()

    }
}
