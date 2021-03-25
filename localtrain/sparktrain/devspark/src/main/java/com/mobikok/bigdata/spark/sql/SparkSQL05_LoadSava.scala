package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}


object SparkSQL05_LoadSava {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称
        // 要求这个对象必须使用val声明
        import spark.implicits._

        // TODO SparkSQL通用的读取和保存

        // TODO 通用的读取
        // RuntimeException: xxxxx/input/user.json is not a Parquet file
        // SparkSQL通用读取的默认数据格式为Parquet列式存储格式，
        //val frame: DataFrame = spark.read.load("input/users.parquet")
        // 如果想要改变读取文件的格式。需要使用特殊的操作
        // TODO 如果读取的文件格式为JSON格式,Spark对JSON文件的格式有要求
        // JSON => JavaScript Object Notation
        // JSON文件的格式要求整个文件满足JSON的语法规则
        // Spark读取文件默认是以行为单位来读取。
        // Spark读取JSON文件时，要求文件中的每一行符合JSON的格式要求
        // 如果文件格式不正确，那么不会发生错误，但是解析结果不正确
        val frame: DataFrame = spark.read.format("json").load("input/user.json")

        //spark.read.json()

        frame.show

        spark.stop
    }
}
