package com.zongteng.ztetl.test.spark.stream.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import com.zongteng.ztetl.util.DateUtil
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
object Orders_stream_util {

  var dim_table = Array("gc_wms_warehouse", "gc_wms_sp_service_channel", "gc_wms_warehouse_physical")

  // 临时表名
  val TEMP_GC_WMS_WAREHOUSE = "temp_gc_wms_warehouse"
  val TEMP_GC_WMS_SP_SERVICE_CHANNEL = "temp_gc_wms_sp_service_channel"
  val TEMP_GC_WMS_WAREHOUSE_PHYSICAL = "temp_gc_wms_warehouse_physical"

  // 实时表名
  val TEMP_GC_WMS_ORDER_OPERATION_TIME = "temp_gc_wms_order_operation_time"
  val TEMP_GC_WMS_ORDER_PHYSICAL_RELATION ="temp_gc_wms_order_physical_relation"


  var flat: String = DateUtil.getNowTime()

  /**
    * 缓存维度表（每天缓存一次）
    * @param sparkSession
    */
  def cacheDimTable(sparkSession: SparkSession) = {

    val nowDate: String = DateUtil.getNowTime()  // 获取当天的时间

    if (nowDate.equals(flat)) {
      // 将表作为临时表加载到内存，缓存起来
      dim_table.foreach((tableName: String) => {
        val temp_table_name = ("temp_" + tableName).toLowerCase

        val sql = s"select * from ods.$tableName where day = $nowDate"
        cacheTable(sparkSession, sql, temp_table_name)
      })

      flat = nextDay()
    }
  }

  var f = true

  /**
    * 从表实时缓存：order_operation_time
    * @param sparkSession
    * @param tableName
    */
  def cacheFactOrderOperationTime(sparkSession: SparkSession, connection: Connection, tableName: String) = {
    // cacheTable(sparkSession, tableName, TEMP_GC_WMS_ORDER_OPERATION_TIME)

//    val sql = s"select * from ods.$tableName"


//
 //    cacheTable(sparkSession, "select * from ods.gc_wms_orders", TEMP_GC_WMS_ORDER_OPERATION_TIME)
  //  cacheTable(sparkSession, "select * from ods.gc_wms_order_operation_time", TEMP_GC_WMS_ORDER_OPERATION_TIME)
//    cacheTable(sparkSession, "select * from ods.gc_wms_order_physical_relation", TEMP_GC_WMS_ORDER_OPERATION_TIME)
//
//    throw new Exception("=======================cacheFactOrderOperationTime==========================")

   // val ods_table = "gc_wms" + "_" + tableName

        if (f) {

          f = false
          val sql = "SELECT * FROM \n" +
            " (SELECT * FROM ods.gc_wms_orders LIMIT 100) AS ods\n"
 //           " LEFT JOIN ods.gc_wms_order_operation_time AS odot ON ods.order_id = odot.order_id"
//            "LEFT JOIN ods.gc_wms_warehouse wh on ods.warehouse_id = wh.warehouse_id\n".replace("ods.gc_wms_warehouse", TEMP_GC_WMS_WAREHOUSE) +
//            "LEFT JOIN ods.gc_wms_sp_service_channel sc on ods.sc_id = sc.sc_id\n".replace("ods.gc_wms_sp_service_channel", TEMP_GC_WMS_SP_SERVICE_CHANNEL) +
//            "LEFT JOIN ods.gc_wms_order_physical_relation opr on opr.order_code = ods.order_code\n"+
//            "LEFT JOIN ods.gc_wms_warehouse_physical wpy on wpy.wp_code = opr.wp_code".replace("ods.gc_wms_warehouse_physical", TEMP_GC_WMS_WAREHOUSE_PHYSICAL)

          sparkSession.sql(sql).show()
        }




//    if (f) {
//
//      f = false
//
//      val table: Table = connection.getTable(TableName.valueOf("gc_wms_order_operation_time"))
//      val scan = new Scan()
//      val filter = new FamilyFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("orders")));
//      scan.setFilter(filter)
//      val resutScanner: ResultScanner = table.getScanner(scan)
//      var i = 0
//      for (r <- resutScanner) {
//        i += 1
//        for (cell <- r.rawCells) {
//          println(i + "  Rowkey-->" + Bytes.toString(r.getRow) + "  " + "Familiy:Quilifier-->" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "  " + "Value-->" + Bytes.toString(CellUtil.cloneValue(cell)))
//        }
//      }
//
//
//    }





  }

  /**
    * 从表实时缓存：order_physical_relation
    * @param sparkSession
    * @param tableName
    */
  def cacheFactOrderPhysicalRelation(sparkSession: SparkSession,tableName: String) = {
    cacheTable(sparkSession, tableName, TEMP_GC_WMS_ORDER_PHYSICAL_RELATION)
  }

  private def cacheTable(sparkSession: SparkSession, sql: String, temp_table_name: String) = {

    // 转换成为dataFrame，创建临时表，cache缓存起来
    sparkSession.sql(sql).toDF().createOrReplaceTempView(temp_table_name)
    sparkSession.table(temp_table_name).cache()

    // 打印展示出来
    sparkSession.read.table(temp_table_name).show(10)
  }


  def nextDay() = {
    val date = new Date()
    val calendar = new GregorianCalendar()
    val format = new SimpleDateFormat("yyyyMMdd")

    calendar.setTime(date)
    calendar.add(Calendar.DATE,1)

    format.format(calendar.getTime())
  }



}
