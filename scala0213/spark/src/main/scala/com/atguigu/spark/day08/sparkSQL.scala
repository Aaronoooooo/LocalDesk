package com.atguigu.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author zxjgreat
 * @create 2020-06-10 20:59
 */
object sparkSQL {
  def main(args: Array[String]): Unit = {
    //创建spark环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // TODO 逻辑操作
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象必须使用val声明
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.json("input/user.json")

    // TODO SQL
    // 将df转换为临时视图
    jsonDF.createOrReplaceTempView("user")
    spark.sql("select * from user").show
    // TODO DSL
    // 如果查询列名采用单引号，那么需要隐式转换
    jsonDF.select("name").show()
    jsonDF.select('age).show()

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 30),
      (2, "lisi", 20),
      (3, "wangwu", 40)
    )
    )
    // TODO RDD <=> DataFrame
    val df: DataFrame = rdd.toDF("id","name","age")
    val rdd1: RDD[Row] = df.rdd
    // TODO RDD <=> DataSet
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val ds: Dataset[User] = userRDD.toDS()
    val rdd2: RDD[User] = ds.rdd
    // TODO DataFrame <=> DataSet
    val dfToDs: Dataset[User] = df.as[User]
    val dsToDf: DataFrame = dfToDs.toDF()

    rdd.foreach(println)
    df.show()
    ds.show
    //释放对象
    spark.stop()
  }
  case class User(id:Int, name:String, age:Int)
}
