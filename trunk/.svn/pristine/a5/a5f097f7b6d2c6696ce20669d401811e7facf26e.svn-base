package com.zongteng.ztetl.etl.ods

import com.zongteng.ztetl.common.MysqlDatabase
import com.zongteng.ztetl.util.stream.SparkStreamCodeUtil


object SparkStream_gc_wms {

  def main(args: Array[String]): Unit = {
    //作业名称
    val appName = "sparkStream_gc_wms_mysql_ods"

    //消费时间间隔
    val interval = 5

    //kafka中topic名称
    val topic_name = "gc_wms"

    //消费组
    //val group_Id = "consumer_gc_wms"
    val group_Id = "consumer_gc_wms_new"

    //mysql数据库名称
    val mysql_database = MysqlDatabase.GC_WMS

    //hbase中ods层表名
    val datasource_name = "gc_wms"

    SparkStreamCodeUtil.getRunCode(appName, interval, topic_name, group_Id, mysql_database, datasource_name)
  }

}
