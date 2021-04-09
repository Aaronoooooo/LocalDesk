package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * @create 2020-06-04 12:33
 */
object Test5_filter {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")
    val sc = new SparkContext(sparkConf)
    //从服务器日志数据apache.log中获取2015年5月17日的请求路径

    //从文件中获取数据
    val dataRDD: RDD[String] = sc.textFile("input/apache.log")

    //将数据切割
    val timeRDD: RDD[String] = dataRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(3)
      }
    )
    //获取所需时间
    val rdd: RDD[String] = timeRDD.filter(
      log => {
        val time: String = log.substring(0, 10)
        time == "17/05/2015"
      }
    )
    //采集打印
    rdd.collect().foreach(println)


    sc.stop()

  }


}
