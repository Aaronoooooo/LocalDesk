package com.mobikok.bigdata.spark.sql

import com.mobikok.bigdata.spark.sql.SparkSQL01_Test.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL01_Test2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-sql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._
    val jsonDF: DataFrame = session.read.json("input/user.json")
    jsonDF.createOrReplaceTempView("user")
    //session.sql("select * from user").show
    //jsonDF.select("name","age").show
    jsonDF.select($"name",$"age").show
    //查询列名采用单引号，那么需要隐式转换
    jsonDF.select('name,'age).show
    val rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List(
      (1, "zhangsan", 20),
      (3, "wangwu", 20),
      (5, "lisi", 40)
    ))
    val df: DataFrame = rdd.toDF("id","name","age")
    val dfToRDD: RDD[Row] = df.rdd
    dfToRDD.foreach(row => {
      println(row(0))
    })

    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    val dsToRDD: RDD[User] = userDS.rdd

    val dfToDS: Dataset[User] = df.as[User]
    val dsToDF: DataFrame = dfToDS.toDF()

    rdd.foreach(println)
    df.show()
    userDS.show()

  }
}
