package com.mobikok.bigdata.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark45_RDD_Operator_Req {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 案例实操

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 统计出每一个省份每个广告被点击数量排行的Top3

        // TODO 1. 获取原始数据
        val dataRDD = sc.textFile("input/agent.log")

        // TODO 2. 将原始数据进行结构的转换，方便统计
        //  (（省份 - 广告）,1)
        val mapRDD : RDD[(String, Int)] = dataRDD.map(
            line => {
                val datas = line.split(" ")
                (datas(1) + "-" + datas(4),1)
            }
        )

        // TODO 3. 将相同key的数据进行分组聚合
        //  (（省份 - 广告）,sum)
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

        // TODO 4. 将聚合后的结果进行结构的转换
        //  ( 省份，（广告，sum） )
        val mapRDD1 = reduceRDD.map{
            case ( key, sum ) => {
                val keys = key.split("-")
                ( keys(0), ( keys(1), sum ) )
            }
        }

        // TODO 5. 将相同的省份的数据分在一个组中
        //  ( 省份，Iterator[（广告1，sum1）,（广告2，sum2）] )
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

        // TODO 6. 将分组后的数据进行排序（降序），取前3 Top3
        // Scala mapValues
        val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
            iter.toList.sortWith(
                (left, right) => {
                    left._2 > right._2
                }
            ).take(3)
        })

        // TODO 7. 将数据采集到控制台打印
        val result: Array[(String, List[(String, Int)])] = sortRDD.collect()
        result.foreach(println)

        sc.stop
    }

}
