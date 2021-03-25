package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkSQL06_LoadSava1_Test {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        val df: DataFrame = session.read.format("json").load("input/user.json")
        //df.write.mode("append").format("json").save("output")
      session.sql("select * from json.`input/user.json`").show
    }
}
