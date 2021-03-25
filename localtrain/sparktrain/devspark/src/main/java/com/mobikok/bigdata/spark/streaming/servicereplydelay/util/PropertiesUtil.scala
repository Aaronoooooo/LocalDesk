package com.mobikok.bigdata.spark.streaming.servicereplydelay.util

import java.util.ResourceBundle

object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    val str: String = PropertiesUtil.getValue("kafka.broker.list")
    println(str)
  }

//  def load(propertieName:String): Properties ={
//    val prop=new Properties();
//    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
//    prop
//  }

  val summer: ResourceBundle = ResourceBundle.getBundle("scrm_dwi_hive/config")
  def getValue( key : String ): String = {
    summer.getString(key)
  }

}
