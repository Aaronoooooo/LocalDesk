package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author zxjgreat
 * @create 2020-06-10 20:59
 */
object sparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
//    // TODO 创建环境对象
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
//    // builder 构建，创建
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//    // 导入隐式转换，这里的spark其实是环境对象的名称
//    // 要求这个对象必须使用val声明
//    import spark.implicits._
//    // TODO 逻辑操作
//
//    val rdd = spark.sparkContext.makeRDD(List(
//      (1, "zhangsan", 30L),
//      (2, "lisi", 20L),
//      (3, "wangwu", 40L)
//    ))
//
//    val df = rdd.toDF("id", "name", "age")
//    df.createOrReplaceTempView("user")
//    //创建UDAF函数
//    val udaf = new MyAvgAgeUDAF
//    //注册到SparkSQL中
//    spark.udf.register("ageAvg",udaf)
//    //定义用户的自定义聚合函数
//    spark.sql("select ageAvg(age) from user").show()
//    //释放对象
//    spark.stop()
//  }
//  // 自定义聚合函数
//  // 1. 继承UserDefinedAggregateFunction
//  // 2. 重写方法
//  class MyAvgAgeUDAF extends UserDefinedAggregateFunction{
//    //输入数据的结构信息:年龄信息
//    override def inputSchema: StructType = {
//      StructType(Array(StructField("age",LongType)))
//    }
//
//    //缓冲区的数据结构信息:年龄的总和，人的数量
//    override def bufferSchema: StructType = {
//      StructType(Array(StructField("age",LongType),StructField("count",LongType)))
//    }
//
//    //输出数据的结构信息
//    override def dataType: DataType = LongType
//
//    //函数的稳定性
//    override def deterministic: Boolean = true
//
//    //函数缓冲区初始化
//    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0) = 0L
//      buffer(1) = 0L
//    }
//
//    //更新缓冲区数据
//    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//      buffer(0) = buffer.getLong(0) + input.getLong(0)
//      buffer(1) = buffer.getLong(1) + 1
//    }
//
//    //合并缓冲区
//    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
//      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
//    }
//
//    //函数的计算
//    override def evaluate(buffer: Row): Any = {
//      buffer.getLong(0) / buffer.getLong(1)
//    }

    //创建spark环境对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //构建对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入隐式转换
    import spark.implicits._
    val rdd: RDD[(Int, String, Long)] = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 30L),
      (2, "lisi", 20L),
      (3, "wangwu", 40L)
    ))
    val df: DataFrame = rdd.toDF("id","name","age")
    df.createOrReplaceTempView("user")
    //创建UDAF函数
    val udaf = new MyAvgAgeUDAF
    //注册函数
    spark.udf.register("avgAge",udaf)
    //使用udaf函数
    spark.sql("select avgAge(age) from user").show()

    spark.stop()

  }
  class MyAvgAgeUDAF extends UserDefinedAggregateFunction{
    //输入数据的类型
    override def inputSchema: StructType = {
      StructType(Array(StructField("age",LongType)))
    }

    //缓冲区数据的类型
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalAge",LongType),
        StructField("count",LongType)
      ))
    }


    //输出数据的类型
    override def dataType: DataType = LongType

    //稳定性
    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L

    }

    //缓冲区更新
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    //合并缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //函数的计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
