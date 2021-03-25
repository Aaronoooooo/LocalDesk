package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SparkSQL10_Save_MySQL {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称

        val frame: DataFrame = spark.read.format("jdbc")
                .option("url", "jdbc:mysql://linux1:3306/spark-sql")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "123123")
                .option("dbtable", "user")
                .load()
        frame.write.format("jdbc")
                .option("url", "jdbc:mysql://linux1:3306/spark-sql")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "123123")
                .option("dbtable", "user1")
                .mode(SaveMode.Append)
                .save()

        spark.stop
    }
}
