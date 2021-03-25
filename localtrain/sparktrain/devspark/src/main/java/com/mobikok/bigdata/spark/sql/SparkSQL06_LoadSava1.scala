package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkSQL06_LoadSava1 {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称
        // 要求这个对象必须使用val声明

        // TODO SparkSQL通用的读取和保存

        // TODO 通用的保存
        val df = spark.read.format("json").load("input/user.json")

        // TODO sparksql默认通用保存的文件格式为parquet
        // 如果想要保存的格式是指定的格式，比如json，那么需要进行对应的格式化操作
        // AnalysisException: path /classes-0213/output already exists.;
        // 如果路径已经存在，那么执行保存操作会发生错误。
        //df.write.format("json").save("output")
        // 如果非得想要在路径已经存在的情况下，保存数据，那么可以使用保存模式
        df.write.mode("append").format("json").save("output")

        spark.stop
    }
}
