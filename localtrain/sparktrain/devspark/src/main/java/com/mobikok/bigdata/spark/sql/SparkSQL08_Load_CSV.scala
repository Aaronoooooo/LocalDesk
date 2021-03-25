package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkSQL08_Load_CSV {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称
        import spark.implicits._

        val frame: DataFrame = spark.read.format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("input/user.csv")
        frame.show

        spark.stop
    }
}
