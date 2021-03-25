package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL01_Test {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称
        // 要求这个对象必须使用val声明
        import spark.implicits._
        // TODO 逻辑操作

        val jsonDF = spark.read.json("input/user.json")

        // TODO SQL
        // 将df转换为临时视图
        jsonDF.createOrReplaceTempView("user")
        spark.sql("select * from user").show

        // TODO DSL
        // 如果查询列名采用单引号，那么需要隐式转换。
        jsonDF.select("name", "age").show
        jsonDF.select($"name", $"age").show
        jsonDF.select('name, 'age).show

        val rdd = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 20),
            (3, "wangwu", 40)
        ))
        // TODO RDD <=> DataFrame
        val df: DataFrame = rdd.toDF("id", "name", "age")
        val dfToRDD: RDD[Row] = df.rdd

        dfToRDD.foreach(row=>{
            println(row(0))
        })

        // TODO RDD <=> DataSet
        val userRDD = rdd.map{
            //case (id, name, age) => {
            //    User(id, name, age)
            //}
            //(id, name, age) =>{
            //    User(id, name, age)
            //}
            case (id,name,age) => {
                User(id,name,age)
            }
        }
        val userDS: Dataset[User] = userRDD.toDS()
        val dsToRdd = userDS.rdd

        // TODO DataFrame <=> DataSet
        val dfToDS: Dataset[User] = df.as[User]
        val dsToDF: DataFrame = dfToDS.toDF()

        rdd.foreach(println)
        df.show()
//        userDS.show
        println("*************************")
        userDS.select($"id", $"age" + 100 as("age")).show()


        // TODO 释放对象
        spark.stop()
    }
    case class User(id:Int, name:String, age:Int)
}
