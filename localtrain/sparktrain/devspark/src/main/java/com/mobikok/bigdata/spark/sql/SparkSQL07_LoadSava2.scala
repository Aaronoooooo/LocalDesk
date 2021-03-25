package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkSQL07_LoadSava2 {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称
        // 要求这个对象必须使用val声明

        // TODO SparkSQL通用的读取和保存

        // TODO 通用的保存
        spark.sql("select * from json.`input/user.json`").show

        spark.stop
    }
}
