package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SparkSQL11_Load_Hive {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建

        // TODO 默认情况下SparkSQL支持本地Hive操作的，执行前需要启用Hive的支持
        // 调用enableHiveSupport方法。
        val spark = SparkSession.builder()
                .enableHiveSupport()
                .config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称

        // 可以使用基本的sql访问hive中的内容
        //spark.sql("create table aa(id int)")
        //spark.sql("show tables").show()
        //spark.sql("load data local inpath 'input/id.txt' into table aa")
        spark.sql("select * from kylin_sales").show


        spark.stop
    }
}
