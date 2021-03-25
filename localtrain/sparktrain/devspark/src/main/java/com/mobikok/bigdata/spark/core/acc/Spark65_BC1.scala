package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark65_BC1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 广播变量

        // join会有笛卡尔乘积效果，数据量会急剧增多。如果有shuffle操作，那么性能会非常低

        // TODO 为了解决join出现性能问题，可以将数据独立出来，防止shuffle操作。
        // 这样的话。会导致数据每一个task会复制一份，那么executor内存中会有大量冗余数据，性能也会受到影响。
        // 所以可以采用广播变量，将数据保存到executor的内存中。
        val rdd1 = sc.makeRDD(List(("a",1),("b",2), ("c",3)))
        val list = List(("a",4),("b",5), ("c",6))

        // （a, (1,4)）,(b, (2,5)), (c, (3,6))
        // ("a",1) => ("a", (1,4)), (b, 2) => ("b", (2,5))
        val rdd2 = rdd1.map{
            case ( word, count1 ) => {

                var count2 = 0
                for ( kv <- list ) {
                    val w = kv._1
                    val v = kv._2
                    if ( w == word ) {
                        count2 = v
                    }
                }

                (word, (count1, count2))
            }
        }

        println(rdd2.collect().mkString(","))

        sc.stop()
    }
}
