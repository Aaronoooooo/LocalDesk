package com.zongteng.ztetl.etl.stream.gc_wms_orders.fact

import java.sql.{Connection, PreparedStatement}

import com.zongteng.ztetl.common.jdbc.JDBCUtil

object OrdersOperate {

  /**
    * 保留n天之内的数据
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val map: Map[String, String] = Map("gc_wms_orders"->"add_time","TmsTracking_sync_order"->"CreationTime")

    assert(args.length > 0, "请输入参数n(1或2或3等等)，保留n天之内的数据，其他的删除")


    var connection: Connection = null

    var delstatement: PreparedStatement = null
    try {

      val day = args(0).toInt

      if (day == 0) {
        println("删除所有的数据")
      } else {
        println(s"删除距今大于等于${day}天的数据")
      }

      connection = JDBCUtil.getConnection(JDBCUtil.jdbc_zt75_streaming)
      connection.setAutoCommit(false)

      map.foreach(x=>{
        // sql
        val sql = s"DELETE FROM ${x._1} WHERE datediff(now(), ${x._2}) >= ?"

        // 预编译
        delstatement = connection.prepareStatement(sql)

        // 赋值为占位符
        delstatement.setInt(1, day)

        // 执行sql
        val i: Int = delstatement.executeUpdate()

        println(s"删除${x._1}表的第${i}条数据")
      })


      // 提交事务
      connection.commit()
    } catch {
      case e: Exception => {
        if (connection != null) connection.rollback() // 回滚
        throw e
      }
    } finally {
      JDBCUtil.close(null, delstatement, connection)
    }

  }

}
