package com.zongteng.ztetl.common

import com.zongteng.ztetl.api.SparkConfig
import com.zongteng.ztetl.util.Log
import org.apache.spark.sql.SparkSession

object Dw_fact_common {

  /**
    * 不缓存值列表
    * @param task
    * @param tableName
    * @param sqlArray
    */

  def getRunCode_Hive_inc_info(task: String, tableName: String,sqlArray: Array[String]): Unit = {
    getRunCode_Hive_inc_info(task,tableName,sqlArray,Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }

  /**
    * dw增量,存放到hive表中
    *
    * @param task
    * @param sqlArray
    * @param tableName
    */
  def getRunCode_Hive_inc_info(task: String, tableName: String,sqlArray: Array[String], datasource_num_id_s: Array[String]): Unit = {

    val logTask: String = Log.start(task)

    try {
      val spark: SparkSession = SparkConfig.init(task)

      // 值列表缓存
      Dw_par_val_list_cache.cacheParValList(spark, datasource_num_id_s)

      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") // 设置动态分区为非严格模式
      spark.sql("set hive.exec.dynamic.partition=true") // 开启动态分区

      //这里是增量导入,先用insert into插入数据
      val insertStr = s"INSERT INTO TABLE dw.$tableName PARTITION(month) \n"
      sqlArray.map(insertStr + _).foreach(spark.sql(_))

      spark.stop()
      Log.end(logTask)
    } catch {
      case ex: Exception => Log.error(logTask, ex)
    }
  }

  /**
    * 不缓存值列表,dw层增量
    * @param task
    * @param tableName
    * @param sqlArray
    */
  def getRunCode_hive_nopartition_inc_Into(task: String, tableName: String, sqlArray: Array[String]) : Unit = {
    getRunCode_hive_nopartition_inc_Into(task, tableName, sqlArray, Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }

  /**
    * dw增量,表为hive表
    * 表为非分区
    * @param task
    * @param tableName
    * @param sqlArray
    */
  def getRunCode_hive_nopartition_inc_Into(task: String, tableName: String, sqlArray: Array[String], datasource_num_id_s: Array[String]) = {

    val logTask: String = Log.start(task)

    try {
      val spark: SparkSession = SparkConfig.init(task)

      // 值列表缓存
      Dw_par_val_list_cache.cacheParValList(spark, datasource_num_id_s)

      //溢出处理
      spark.sql("set hive.auto.convert.join = false")

      // 增量插入数据,先用insert into
      val insertStr = s"INSERT INTO TABLE dw.$tableName  \n"

      sqlArray.map(insertStr + _).foreach(spark.sql(_))

      spark.stop()
      Log.end(logTask)
    } catch {
      case ex: Exception => Log.error(logTask, ex)
    }
  }


  /**
    * 不缓存值列表
    * @param task
    * @param tableName
    * @param sqlArray
    */
  def getRunCode_hive_full_Into(task: String, tableName: String, sqlArray: Array[String]) : Unit = {
    getRunCode_hive_full_Into(task, tableName, sqlArray, Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }

  /**
    * 事实表：通过INSERT INTO方式全量覆盖dw层数据，先删除表，再导入数据。
    * @param task
    * @param tableName
    * @param sqlArray
    * @param datasource_num_id_s
    */
  def getRunCode_hive_full_Into(task: String, tableName: String, sqlArray: Array[String], datasource_num_id_s: Array[String]) = {

    val logTask: String = Log.start(task)

    try {
      val spark: SparkSession = SparkConfig.init(task)

      // 先删除表，创建表
      DwCreateTable.createTable(spark, tableName)

      // 值列表缓存
      Dw_par_val_list_cache.cacheParValList(spark, datasource_num_id_s)

      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") // 设置动态分区为非严格模式
      spark.sql("set hive.exec.dynamic.partition=true") // 开启动态分区

      //因为事实表每天都有新建表,所以可以用insert into循环插入数据
      val insertStr = s"INSERT INTO TABLE dw.$tableName PARTITION(month) \n"

      sqlArray.map(insertStr + _).foreach(selectSql => {
        println(selectSql)
        spark.sql(selectSql)
      })

      spark.stop()
      Log.end(logTask)
    } catch {
      case ex: Exception => {
        Log.error(logTask, ex)
      }
    }
  }

  /**
    * 不缓存值列表
    * @param task
    * @param tableName
    * @param sqlArray
    */
  def getRunCode_hive_nopartition_full_Into(task: String, tableName: String, sqlArray: Array[String]) : Unit = {
    getRunCode_hive_nopartition_full_Into(task, tableName, sqlArray, Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }

  /**
    * 事实表：通过INSERT INTO方式全量覆盖dw层数据，所以执行之前要把表全部删除。
    * 表为非分区
    * @param task
    * @param tableName
    * @param sqlArray
    */
  def getRunCode_hive_nopartition_full_Into(task: String, tableName: String, sqlArray: Array[String], datasource_num_id_s: Array[String]) = {

    val logTask: String = Log.start(task)

    try {
      val spark: SparkSession = SparkConfig.init(task)

      // 先删除表，创建表
      DwCreateTable.createTable(spark, tableName)

      // 值列表缓存
      Dw_par_val_list_cache.cacheParValList(spark, datasource_num_id_s)

      //溢出处理
      spark.sql("set hive.auto.convert.join = false")

      // 因为事实表每天都有新建表,所以可以用insert into循环插入数据
      val insertStr = s"INSERT INTO TABLE dw.$tableName  \n"

      sqlArray.map(insertStr + _).foreach(spark.sql(_))

      spark.stop()
      Log.end(logTask)
    } catch {
      case ex: Exception => Log.error(logTask, ex)
    }
  }
}
