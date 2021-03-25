package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark61_Acc1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO 使用累加器完成数据的累加
        val rdd = sc.makeRDD(List(
            1,2,3,4
        ))

        // TODO 声明累加器变量
        val sum: LongAccumulator = sc.longAccumulator("sum")

        rdd.foreach(
            num => {
                // TODO 使用累加器
                sum.add(num)
            }
        )

        // TODO  获取累加器的结果
        println("结果为 = " + sum.value)



        sc.stop()

    }
}
