package com.zongteng.ztetl.etl.ods

import com.zongteng.ztetl.common.MysqlDatabase
import com.zongteng.ztetl.util.stream.SparkStreamCodeUtilGcTcms

object SparkStream_gc_tcms {

  //作业名称
  val appName = "sparkStream_collect_data_mysql_ods_gc_tcms"
  //消费时间间隔
  val interval = 10

  //kafka中topic名称
  val topic_name = Array("kafkaOffset4", "kafkaOffset2")
  //val topic_name = Array("gc_wms")

  //消费组
  //val group_Id = "consumer_gc_wms"
  val group_Id = "consumer_gc_tcms_DDDD"

  //mysql数据库名称
  val mysql_database = MysqlDatabase.GC_WMS

  //hbase中ods层表名
  val datasource_name = "gc_wms"


  def main(args: Array[String]): Unit = {

    SparkStreamCodeUtilGcTcms.getRunCode(appName, interval, topic_name, group_Id, mysql_database, datasource_name)

  }

}
