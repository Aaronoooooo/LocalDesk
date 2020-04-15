package com.zongteng.ztetl.common

import com.zongteng.ztetl.api.SparkConfig
import com.zongteng.ztetl.util.Log
import org.apache.spark.sql.SparkSession

object Dw_dim_common {

  /**
    * 维度表dw全量,不用缓存值列表
    *
    * @param task      任务编号
    * @param sqlArray  要执行的sql语句
    * @param tableName dw层表名
    */
  def getRunCode_full(task: String, tableName: String, sqlArray: Array[String]): Unit = {
    getRunCode_full(task, tableName, sqlArray, Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }

  /**
    * 维度表dw全量
    *
    * @param task      任务编号
    * @param sqlArray  要执行的sql语句
    * @param tableName dw层表名
    */
  def getRunCode_full(task: String, tableName: String, sqlArray: Array[String], datasource_num_id_s: Array[String]): Unit = {

    val logTask: String = Log.start(task)

    try {
      val spark: SparkSession = SparkConfig.init(task)

      // 先删除表，创建表
      DwCreateTable.createTable(spark, tableName)

      // 值列表缓存
      Dw_par_val_list_cache.cacheParValList(spark, datasource_num_id_s)

      //因为配置表没有删除表重建,所以要用overwrite的方式
      var insertStr:String=null
      if (tableName.startsWith("par")) {
        insertStr = s"INSERT  OVERWRITE TABLE dw.$tableName  " + sqlArray.mkString("\n  union all \n")
        spark.sql(insertStr)
      } else {
        insertStr = s"insert into table dw.$tableName  "

        sqlArray.map(insertStr + _).foreach(spark.sql(_))
      }

      spark.stop()
      Log.end(logTask)
    } catch {
      case ex: Exception => Log.error(logTask, ex)
    }
  }

  /**
    * 通过dw层维度表名获取最新的维度表数据（不一定是今天的数据）
    *
    * @param table ods层表名
    * @param alias 子查询表的别名
    *
    */
  def getDimSql(table: String, alias: String) = {

    val sql = "(SELECT * FROM (\n" +
      s"SELECT *, rank() over (order by day desc) AS day_rank FROM ods.$table \n" +
      s") dim WHERE day_rank = 1) AS $alias"
    sql
  }
}
