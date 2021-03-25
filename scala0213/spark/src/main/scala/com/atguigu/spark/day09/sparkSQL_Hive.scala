package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object sparkSQL_Hive {

  def main(args: Array[String]): Unit = {


    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象必须使用val声明
    import spark.implicits._

    //内置Hive
//    spark.sql("create table aa(id int)")
//    spark.sql("show tables").show()

    //外部Hive
    spark.sql("show databases").show()


    spark.stop()

  }
}
