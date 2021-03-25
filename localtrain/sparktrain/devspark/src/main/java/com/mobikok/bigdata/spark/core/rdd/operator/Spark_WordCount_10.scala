package com.mobikok.bigdata.spark.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount_10 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 1, groupBy
        // TODO 2, groupByKey
        // TODO 3, reduceByKey
        // TODO 4, aggregateByKey
        // TODO 5, foldByKey
        // TODO 6, combineByKey
        // TODO 7. countByKey
        // TODO 8. countByValue
        val rdd = sc.makeRDD(List("a", "a", "a", "b", "b"))

        // TODO 9. reduce : (Map, Map) => Map
        // word -> count
        val mapRDD: RDD[Map[String, Int]] = rdd.map(word=>Map[String, Int]((word,1)))

//        val map = mapRDD.reduce(
//            ( map1, map2 ) => {
//                map1.foldLeft(map2)(
//                    ( map, kv ) => {
//                        val word = kv._1
//                        val count = kv._2
//                        map.updated( word, map.getOrElse(word, 0) + count )
//                    }
//                )
//            }
//        )
//        println(map)
        // TODO 10. fold
//        val map = mapRDD.fold( Map[String, Int]() )(
//            ( map1, map2 ) => {
//                map1.foldLeft(map2)(
//                    ( map, kv ) => {
//                        val word = kv._1
//                        val count = kv._2
//                        map.updated( word, map.getOrElse(word, 0) + count )
//                    }
//                )
//            }
//        )
//        println(map)

        // TODO 11. aggregate
        val map = rdd.aggregate(Map[String, Int]())(
            (map, k) => {
                map.updated( k, map.getOrElse(k, 0) + 1 )
            },
            ( map1, map2 ) => {
                map1.foldLeft(map2)(
                    ( map, kv ) => {
                        val word = kv._1
                        val count = kv._2
                        map.updated( word, map.getOrElse(word, 0) + count )
                    }
                )
            }
        )
        println(map)


        sc.stop
    }
}
