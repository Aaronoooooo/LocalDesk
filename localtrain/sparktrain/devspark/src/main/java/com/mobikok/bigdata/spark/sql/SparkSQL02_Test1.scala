package com.mobikok.bigdata.spark.sql

import com.mobikok.bigdata.spark.sql.SparkSQL01_Test.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL02_Test1 {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称
        // 要求这个对象必须使用val声明
        import spark.implicits._
        // TODO 逻辑操作

        val rdd = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 20),
            (3, "wangwu", 40)
        ))

        val df = rdd.toDF("id", "name", "age")
        df.createOrReplaceTempView("user")
//        val ds: Dataset[Row] = df.map(row => {
//            val id = row(0)
//            val name = row(1)
//            val age = row(2)
//            Row(id, "name : " + name, age)
//        })
//        val userRDD = rdd.map{
//            case (id, name, age) => {
//                User(id, name, age)
//            }
//        }
//        val userDS: Dataset[User] = userRDD.toDS()
//        val newDS = userDS.map(user=>{
//            User( user.id, "name:" + user.name, user.age )
//        })
//        newDS.show

        // TODO 使用 自定义函数在SQL中完成数据的转换操作
        spark.udf.register("addName",(x:String)=> "Name:"+x)
        spark.udf.register("changeAge",(x:Int)=> 18)

        spark.sql("select addName(name), changeAge(age) from user").show

        // totalage, count

        spark.stop()
    }
    case class User(id:Int, name:String, age:Int)
}
