package com.mobikok.bigdata.spark.core.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark62_Acc2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO 累加器 ： 分布式共享只写变量

        // 所谓的累加器，一般的作用为累加（数值增加，数据的累加）数据

        // 1. 将累加器变量注册到spark中
        // 2. 执行计算时。spark会将累加器发送到executor执行计算
        // 3. 计算完毕后，executor会将累加器的计算结果返回到driver端。
        // 4. driver端获取到多个累加器的结果，然后两两合并。最后得到累加器的执行结果。

        // TODO 使用累加器完成数据的累加
        val rdd = sc.makeRDD(List(
            1,2,3,4
        ))

        // TODO 声明累加器变量
        val sum: LongAccumulator = sc.longAccumulator("sum")
        //sc.doubleAccumulator()
        //sc.collectionAccumulator()

        rdd.foreach(
            num => {
                // TODO 使用累加器
                sum.add(num)
                // 1
                // 2，
                // 3，
                // 4
            }
        )

        // TODO  获取累加器的结果
        println("结果为 = " + sum.value)



        sc.stop()

    }
}
