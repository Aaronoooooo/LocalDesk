package com.zongteng.ztetl.etl.stream.gc_wms_orders.fact

import java.sql.{Connection, PreparedStatement}

import com.zongteng.ztetl.common.jdbc.JDBCUtil

/**
  * 初始化mysql的数据，先把表删除了
  */
object OrdersDataInit {

  def main(args: Array[String]): Unit = {

    var connection: Connection = null

    var deleteStatement: PreparedStatement = null

    try {

      connection = JDBCUtil.getConnection(JDBCUtil.jdbc_zt75_streaming)
      connection.setAutoCommit(false)

      // sql
      val sql_1 = "TRUNCATE TABLE gc_wms_orders"
      val sql_2 = "TRUNCATE TABLE gc_wms_order_physical_relation"
      val sql_3 = "TRUNCATE TABLE gc_wms_order_operation_time"

      val sql_4 = "TRUNCATE TABLE staging_gc_wms_orders"
      val sql_5 = "TRUNCATE TABLE staging_gc_wms_order_physical_relation"
      val sql_6 = "TRUNCATE TABLE staging_gc_wms_order_operation_time"

      // 预编译
      deleteStatement = connection.prepareStatement(sql_1)
      deleteStatement.addBatch(sql_2)
      deleteStatement.addBatch(sql_3)
      deleteStatement.addBatch(sql_4)
      deleteStatement.addBatch(sql_5)
      deleteStatement.addBatch(sql_6)

      // 单独执行sql
      // 删除gc_wms_orders
      val i: Int = deleteStatement.executeUpdate()

       // 执行批处理sql
      // 删除gc_wms_order_physical_relation、gc_wms_order_operation_time
      val num: Array[Int] = deleteStatement.executeBatch()


      if (num.size == 5) {
        println("删除gc_wms_orders、gc_wms_order_physical_relation、gc_wms_order_operation_time表的全部数据")
      }

      // 提交事务
      connection.commit()
    } catch {
      case e: Exception => {
        if (connection != null) connection.rollback() // 回滚
        throw e
      }
    } finally {
      JDBCUtil.close(null, deleteStatement, connection)
    }


  }

}
