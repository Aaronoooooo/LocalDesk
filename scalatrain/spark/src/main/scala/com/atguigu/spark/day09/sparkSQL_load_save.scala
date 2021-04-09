package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}


object sparkSQL_load_save {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象必须使用val声明
    import spark.implicits._
    // TODO 逻辑操作

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
val df: DataFrame = spark.read.format("json").load("input/user.json")


    // TODO sparksql默认通用保存的文件格式为parquet

    // 如果想要保存的格式是指定的格式，比如json，那么需要进行对应的格式化操作
    // AnalysisException: path /classes-0213/output already exists.;
    // 如果路径已经存在，那么执行保存操作会发生错误。
    //df.write.format("json").save("output")
    // 如果非得想要在路径已经存在的情况下，保存数据，那么可以使用保存模式
    df.write.mode("append").format("json").save("output")

    spark.stop()
  }
}
