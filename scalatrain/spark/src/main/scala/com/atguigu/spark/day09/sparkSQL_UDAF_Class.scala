package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}


object sparkSQL_UDAF_Class {

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
      (1, "zhangsan", 30L),
      (2, "lisi", 20L),
      (3, "wangwu", 40L)
    ))

    val df = rdd.toDF("id", "name", "age")
    val ds = df.as[User]
    // TODO 创建UDAF函数
    val udaf = new MyAvgAgeUDAFClass
    // TODO 在SQL中使用聚合函数

    // 因为聚合函数是强类型，那么sql中没有类型的概念，所以无法使用
    // 可以采用DSL语法方法进行访问
    // 将聚合函数转换为查询的列让DataSet访问。
    ds.select(udaf.toColumn).show()

    spark.stop()


  }
  case class User(id: Int, name: String, age: Long)
  case class AvgBuffer(var totalAge : Long ,var count : Long)
  // 自定义聚合函数 - 强类型
  // 1. 继承Aggregator, 定义泛型
  //      -IN : 输入数据的类型 User
  //      BUF : 缓冲区的数据类型 AvgBuffer
  //      OUT : 输出数据的类型 Long
  // 2. 重写方法
  class MyAvgAgeUDAFClass extends Aggregator[User,AvgBuffer,Long]{
    //初始值
    override def zero: AvgBuffer = {
      AvgBuffer(0L,0L)
    }

    //聚合数据
    override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
      b.totalAge = b.totalAge + a.age
      b.count = b.count + 1
      b
    }

    //合并
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
      b1.totalAge = b1.totalAge + b2.totalAge
      b1.count = b1.count + b2.count
      b1
    }

    //结果
    override def finish(reduction: AvgBuffer): Long = {
      reduction.totalAge / reduction.count
    }

    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
