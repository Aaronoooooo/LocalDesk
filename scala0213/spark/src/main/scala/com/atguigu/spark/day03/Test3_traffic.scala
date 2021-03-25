package com.atguigu.spark.day03

import org.apache.spark.rdd.{CoGroupedRDD, HadoopRDD, JdbcRDD, NewHadoopRDD, PartitionPruningRDD, RDD, ShuffledRDD, UnionRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zxjgreat
 * @create 2020-06-04 11:23
 */
object Test3_traffic {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    //从服务器日志数据apache.log中获取每个时间段访问量

    //从文件获取数据
    val logRDD: RDD[String] = sc.textFile("input/apache.log")

    //取出每个数据的时间
    val dataRDD: RDD[String] = logRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(3)
      }
    )
    //按小时分组
    val timeRDD: RDD[(String, Iterable[String])] = dataRDD.groupBy(
      time => {
        time.substring(0, 13)
      }
    )
    //求出每个小时的访问量
    val rdd: RDD[(String, Int)] = timeRDD.map {
      case (hour, list) => {
        (hour, list.size)
      }
    }
    //采集打印
    rdd.collect().foreach(println)
    sc.stop()



  }

}
