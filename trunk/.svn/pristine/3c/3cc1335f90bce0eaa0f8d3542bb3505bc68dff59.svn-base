package com.zongteng.ztetl.etl.ods

import com.zongteng.ztetl.common.MysqlDatabase
import com.zongteng.ztetl.util.stream.SparkStreamCodeUtil

object SparkStream_gc_wms_ibl {
  def main(args: Array[String]): Unit = {
    //作业名称
    val appName = "sparkStream_gc_wms_ibl_mysql_ods"

    //消费时间间隔
    val interval = 5

    //kafka中topic名称
    val topic_name = "gc_wms_ibl"

    //消费组
    val group_Id = "consumer_gc_wms_ibl"

    //mysql数据库名称
    val mysql_database = MysqlDatabase.GC_WMS_IBL

    //hbase中ods层表名
    val datasource_name = "gc_wms_ibl"

    SparkStreamCodeUtil.getRunCode(appName, interval, topic_name, group_Id, mysql_database, datasource_name)
  }
}
