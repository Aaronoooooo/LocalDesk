package com.zongteng.ztetl.common

import com.zongteng.ztetl.api.SparkConfig
import com.zongteng.ztetl.util.Log
import org.apache.spark.sql.SparkSession

object DmCommon {
  /**
    * dm层公共类
    *
    * @param task      任务编号
    * @param sqlArray  要执行的sql语句
    * @param tableName dw层表名
    */
  def getRunCodeFull(task: String, tableName: String, sqlArray: Array[String]): Unit = {

    val logTask: String = Log.start(task)

    try {
      val spark: SparkSession = SparkConfig.init(task)

      //spark.sql("set hive.auto.convert.join = false")

      //因为DM层表没有删除表重建,所以要用overwrite的方式
      val insertStr = s"insert overwrite table dm.$tableName  " + sqlArray.mkString("\n union all \n ")
      //println(insertStr)
      spark.sql(insertStr)

      spark.stop()
      Log.end(logTask)
    } catch {
      case ex: Exception => Log.error(logTask, ex)
    }
  }
}
