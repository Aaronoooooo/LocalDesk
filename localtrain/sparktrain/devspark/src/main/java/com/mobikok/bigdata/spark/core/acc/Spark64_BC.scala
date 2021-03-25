package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark64_BC {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 广播变量

        // join会有笛卡尔乘积效果，数据量会急剧增多。如果有shuffle操作，那么性能会非常低
        val rdd1 = sc.makeRDD(List(("a",1),("b",2), ("c",3)))
        val rdd2 = sc.makeRDD(List(("a",1),("b",2), ("c",3)))

        // （a, (1,1)）,(b, (2,2)), (c, (3,3))
        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        println(joinRDD.collect().mkString(","))

        sc.stop()
    }
}
