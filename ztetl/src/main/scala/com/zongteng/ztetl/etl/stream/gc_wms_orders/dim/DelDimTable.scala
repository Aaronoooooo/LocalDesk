package com.zongteng.ztetl.etl.stream.gc_wms_orders.dim

import java.sql.{Connection, Statement}

import com.zongteng.ztetl.common.jdbc.JDBCUtil

/**
  * 数据导出到维度表之前，先对纬度表先进行清空操作
  *
  * TRUNCATE TABLE gc_wms_warehouse
  * TRUNCATE TABLE gc_wms_sp_service_channel
  * TRUNCATE TABLE gc_wms_warehouse_physical
  */
object DelDimTable {

  def main(args: Array[String]): Unit = {

    val connection: Connection = JDBCUtil.getConnection(JDBCUtil.jdbc_zt75_streaming)

    var delete_statement: Statement = null
    val deleteSql1 = "TRUNCATE TABLE gc_wms_warehouse"
    val deleteSql2 = "TRUNCATE TABLE gc_wms_sp_service_channel"
    val deleteSql3 = "TRUNCATE TABLE gc_wms_warehouse_physical"

    try {
      connection.setAutoCommit(false)

      delete_statement = connection.createStatement()

      delete_statement.addBatch(deleteSql1)
      delete_statement.addBatch(deleteSql2)
      delete_statement.addBatch(deleteSql3)

      val ints: Array[Int] = delete_statement.executeBatch()

      assert(ints.length == 3, "程序有异常，导致数据不能被全部删除，请检查")
      if (ints.length == 3) {
        println("gc_wms_warehouse、gc_wms_sp_service_channel、gc_wms_warehouse_physical数据全部被删除")
      }

      connection.commit()
    } catch {
      case e: Exception => {
        if (connection != null) connection.rollback()
        e.printStackTrace()
      }
    } finally {
      JDBCUtil.close(null, delete_statement, connection)
    }


  }


}
