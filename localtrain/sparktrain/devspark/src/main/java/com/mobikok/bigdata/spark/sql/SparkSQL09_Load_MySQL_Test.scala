package com.mobikok.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SparkSQL09_Load_MySQL_Test {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        //val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        //val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("SparkSQL")
          .getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称

        spark.read.format("jdbc")
                .option("url", "jdbc:mysql://flydiysz.cn:30333/test")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "canal")
                .option("password", "canal")
                .option("dbtable", "orgmodule")
                .load().show


        val props: Properties = new Properties()
        props.setProperty("user", "canal")
        props.setProperty("password", "canal")
        val df: DataFrame = spark.read
          .jdbc("jdbc:mysql://flydiysz.cn:30333/test", "usermodule", props)
        df.show

        //df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://flydiysz.cn:30333/test", "usermodule", props)

        spark.stop
    }
}
