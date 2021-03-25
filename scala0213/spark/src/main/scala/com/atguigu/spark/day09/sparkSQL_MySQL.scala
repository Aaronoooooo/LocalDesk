package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object sparkSQL_MySQL {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象必须使用val声明
    import spark.implicits._

    val frame: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/people")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "Zxiujun0219.")
      .option("dbtable", "user")
      .load()
    frame.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/people")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "Zxiujun0219")
      .option("dbtable", "user1")
      .save()



    spark.stop()

  }
}
