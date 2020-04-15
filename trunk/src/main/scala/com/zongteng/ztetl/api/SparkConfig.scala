package com.zongteng.ztetl.api

import com.zongteng.ztetl.util.PropertyFile
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds




/**
 * 
 * SPARK CONFIG 类
 * @author panmingwen
 */

 object SparkConfig{


  /**
    * 创建SparkSession
    */
  def init(appName:String):SparkSession={
    val master = PropertyFile.getProperty("master");
    val spark = SparkSession.builder().appName(appName).master(master).config("spark.executor.heartbeatInterval","180s").config("spark.network.timeout","190s").getOrCreate()
    return spark;
  }

  def getConf( appName: String):SparkConf={
    val master=PropertyFile.getProperty("master");
    val conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.kryoserializer.buffer.max","1024m")
    return conf;
  }
  
  /**
   * 创建sparkContext 对象
   */
  def getContext(conf:SparkConf):SparkContext={
        val context=new SparkContext(conf)

        return context;
  }
  
  
  /**
   * 创建SparkSession
   *  @author panmingwen
   */
  def getSqlContext(conf:SparkConf):SparkSession={
      val builder=SparkSession.builder().config(conf).enableHiveSupport()
      val spark =builder.getOrCreate();
      return spark;
  }

  /**
    * 创建SparkSession
    *  @author panmingwen
    */
  def getSqlContext(appName:String):SparkSession={
    val master=PropertyFile.getProperty("master")
    val mongodburi=PropertyFile.getProperty("mongodb_uri");
    val spark=SparkSession.builder()
      .appName(appName).master(master)
      .config("spark.mongodb.input.uri",mongodburi)//读mongodb collection
      .config("spark.mongodb.output.uri",mongodburi)//写mongodb collection
      .config("spark.default.parallelism", "500")
      .getOrCreate()
    return spark;
  }

  /**
   * 
   * 创建SPARK STREAMING
   * 
   */
  def getStreamingContext(appName:String, interval:Int):StreamingContext={

    val conf = getConf(appName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.max", "2040m")
      val ssc=new StreamingContext(conf,Seconds(interval));
      return ssc;
  }

  /**
    *
    * @param appName
    * @param interval
    * @param sparkConf
    * @return
    */
  def getStreamingContext(appName: String, interval: Int, sparkConf: SparkConf)  = {
    val conf = sparkConf
    conf.setAppName(appName)
    conf.setMaster(PropertyFile.getProperty("master"))
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryoserializer.buffer.max", "2040m")
    new StreamingContext(conf, Seconds(interval));
  }



  
}