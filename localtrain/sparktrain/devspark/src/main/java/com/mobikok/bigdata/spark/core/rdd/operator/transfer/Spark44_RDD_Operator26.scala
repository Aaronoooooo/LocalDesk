package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark44_RDD_Operator26 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD - 算子（方法）

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(
            List(
                ("a",1), ("b", 2),("c",3),("c",4)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("a",4), ("b", 5), ("b", 6)
            )
        )

        // TODO leftOuterJoin
        // TODO rightOuterJoin
        //val result: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
       // rdd1.rightOuterJoin()

        // cogroup
        val result: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

        result.collect().foreach(println)

        sc.stop
    }

}
