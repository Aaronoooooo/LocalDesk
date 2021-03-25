package com.zongteng.ztetl.etl.stream.gc_wms_orders.fact

import com.zongteng.ztetl.common.MysqlDatabase

object StreamGcWmsOrders {

  def main(args: Array[String]): Unit = {

    // 作业名称
    val appName = "Order_stream_gc_wms"

    // 消费时间间隔
    val interval = 5

    // kafka中topic名称
    val topic_name = Array("gc_wms")
    //val topic_name = "zy_wms"

    // 消费组
    val group_Id = "StreamGcWmsOrders"

    // mysql数据库名称（数据的来源）
    val mysql_database = MysqlDatabase.GC_WMS

    // 数据来源简称（例如：gc_oms）
    val datasource_name = "gc_wms"

    StreamGcWmsOrdersUtil.getRunCode(appName, interval, topic_name, group_Id, mysql_database, datasource_name)
  }

}
