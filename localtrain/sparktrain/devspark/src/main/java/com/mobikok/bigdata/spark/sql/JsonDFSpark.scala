package com.mobikok.bigdata.spark.sql

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.slf4j.{Logger, LoggerFactory}
import java.util.LinkedHashMap

import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object JsonDFSpark {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(JsonDFSpark.getClass)
    val conf = new SparkConf().setMaster("local[*]").setAppName("JsonDFSpark")
    val ssc = new StreamingContext(conf, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_streaming",
      //      "auto.offset.reset" -> "earliest",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("chat-records")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )



    /**
     * 方法一：处理JSON字符串为case class 生成RDD[case class] 然后直接转成DataFrame
     */
        stream.map(record => handleMessage2CaseClass(record.value())).foreachRDD(rdd => {
          val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
          val df = spark.createDataFrame(rdd)
          import org.apache.spark.sql.functions._
          import spark.implicits._
          df.createOrReplaceTempView("service_table")
          print("方法一：处理JSON字符串为case class 生成RDD[case class] 然后直接转成DataFrame")
          spark.sql("select * from service_table").show

          df.select(date_format($"time".cast(DateType), "yyyyMMdd").as("day"), $"*")
                  .write.mode(SaveMode.Append)
                  .partitionBy("namespace", "day")
                  .parquet("/Users/shirukai/Desktop/HollySys/Repository/learn-demo-spark/data/Streaming")
        })


    /**
     * 方法二：处理JSON字符串为Tuple 生成RDD[Tuple] 然后转成DataFrame
     */
    //    stream.map(record => handleMessage2Tuples(record.value())).foreachRDD(rdd => {
    //      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    //      import spark.implicits._
    //      val df = rdd.toDF("id", "value", "time", "valueType", "region", "namespace")
    //      df.show()
    //    })

    /**
     * 方法三：处理JSON字符串为Row 生成RDD[Row] 然后通过schema创建DataFrame
     */
    //    val schema = StructType(Array(
    //      StructField("user", StringType),
    //      StructField("chatService", StringType),
    //      StructField("user_service_id", StringType),
    //      StructField("service_user_id", StringType),
    //      StructField("time", StringType)
    //    ))
    //    stream.map(record => handlerMessage2Row(record.value())).foreachRDD(rdd => {
    //      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    //      val df = spark.createDataFrame(rdd, schema)
    //      print("方法三：处理JSON字符串为Row 生成RDD[Row] 然后通过schema创建DataFrame")
    //      df.show()
    //    })

    /**
     * 方法四：直接将 RDD[String] 转成DataSet 然后通过schema转换
     */
//            val schema = StructType(List(
//              StructField("namespace", StringType),
//              StructField("id", StringType),
//              StructField("region", StringType),
//              StructField("time", StringType),
//              StructField("value", StringType),
//              StructField("valueType", StringType))
//            )
//            stream.map(record => record.value()).foreachRDD(rdd => {
//              val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
//              import spark.implicits._
//              val ds = spark.createDataset(rdd)
//              ds.select(from_json('value.cast("string"), schema) as "value").select($"value.*").show()
//            })

    /**
     * 方法五：直接将 RDD[String] 转成DataSet 然后通过read.json转成DataFrame
     */
    //    stream.map(record => record.value()).foreachRDD(rdd => {
    //      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    //      import spark.implicits._
    //      val df = spark.read.json(spark.createDataset(rdd))
    //      print("方式五:RDD[String] 转成DataSet 然后通过read.json转成DataFrame")
    //      df.show()
    //    })

    /**
     * 补充：处理[]数组格式的json字符串，方法一：通过handleMessage自定义方法处理JSON字符串为Array[case class]，
     * 然后通过flatmap展开，再通过foreachRDD拿到RDD[case class]格式的RDD，最后直接转成DataFrame。
     */
    //    stream.map(record => handleMessage(record.value())).flatMap(x=>x).foreachRDD(rdd => {
    //      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    //      val df = spark.createDataFrame(rdd)
    //      df.show()
    //    })

    /**
     * 补充：处理[]数组格式的json字符串，方法二：第二种：直接处理RDD[String]，创建DataSet，
     * 然后通过Spark SQL 内置函数from_json和指定的schema格式化json数据，
     * 再通过内置函数explode展开数组格式的json数据，最后通过select json中的每一个key，获得最终的DataFrame
     */
    //    val schema = StructType(List(
    //      StructField("namespace", StringType),
    //      StructField("id", StringType),
    //      StructField("region", StringType),
    //      StructField("time", StringType),
    //      StructField("value", StringType),
    //      StructField("valueType", StringType))
    //    )
    //    stream.map(record => record.value()).foreachRDD(rdd => {
    //      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    //      import spark.implicits._
    //      val ds = spark.createDataset(rdd)
    //      import org.apache.spark.sql.functions._
    //      val df = ds.select(from_json('value, ArrayType(schema)) as "value").select(explode('value)).select($"col.*")
    //      df.show()
    //    })
    ssc.start()
    ssc.awaitTermination()
  }

  def handleMessage(jsonStr: String): Array[KafkaMessage] = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[Array[KafkaMessage]])
  }

  def handleMessage2CaseClass(jsonStr: String): KafkaMessage = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[KafkaMessage])
  }

  def handleMessage2Tuples(jsonStr: String): (String, String, String, String, String, String) = {
    import scala.collection.JavaConverters._
    val list = JSON.parseObject(jsonStr, classOf[LinkedHashMap[String, Object]]).asScala.values.map(x => String.valueOf(x)).toList
    list match {
      case List(v1, v2, v3, v4, v5, v6) => (v1, v2, v3, v4, v5, v6)
    }
  }

  def handlerMessage2Row(jsonStr: String): Row = {
    import scala.collection.JavaConverters._
    val array = JSON.parseObject(jsonStr, classOf[LinkedHashMap[String, Object]]).asScala.values.map(x => String.valueOf(x)).toArray
    Row(array: _*)
  }
}

//case class com.mobikok.bigdata.spark.streaming.rep2.KafkaMessage(time: String, namespace: String, id: String, region: String, value: String, valueType: String)
case class KafkaMessage(user: String, chatService: String, user_service_id: String, service_user_id: String, time: String)