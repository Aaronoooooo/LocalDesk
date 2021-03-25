package com.zongteng.ztetl.common.jdbc

import java.sql._
import com.mchange.v2.c3p0.ComboPooledDataSource
import javax.sql.DataSource

object JDBCUtil extends Serializable {

  // jdbc的配置名字（c3po配置文件）
  val jdbc_zt72_test = "zt72_test"
  val jdbc_gc_wms = "gc_wms"
  val jdbc_zt75_streaming = "zt75_streaming"

  private var ds_zt72_test: DataSource = null
  private var ds_gc_wms: DataSource = null
  private var ds_zt75_streaming: DataSource = null

  // 获取连接
  def getConnection(configName: String) = {

    var connection: Connection = null

    configName match {
      case JDBCUtil.jdbc_zt72_test => {
        ds_zt72_test = getZt72TestDataSource(jdbc_zt72_test, jdbc_zt72_test)
        connection = ds_zt72_test.getConnection
      }
      case JDBCUtil.jdbc_gc_wms => {
        ds_gc_wms = getGcWmsDataSource(jdbc_gc_wms, jdbc_gc_wms)
        connection = ds_gc_wms.getConnection()
      }
      case JDBCUtil.jdbc_zt75_streaming => {
        ds_zt75_streaming = getZt75StreamingDataSource(jdbc_zt75_streaming, jdbc_zt75_streaming)
        connection = ds_zt75_streaming.getConnection()
      }
      case _ => throw new Exception(s"没有${configName}这个的数据连接的配置信息，请在c3p0-config.xml文件下配置")
    }

    connection
  }

  /**
    * 单例模式，避免重复创建连接池对象
    * @param jdbc_name
    * @param lock
    * @return
    */
  def getZt72TestDataSource(jdbc_name: String, lock: String) = {

    if (ds_zt72_test == null) {
      lock.synchronized {
        if (ds_zt72_test == null) {
          ds_zt72_test = new ComboPooledDataSource(jdbc_name);
        }
      }
    }

    ds_zt72_test
  }

  def getGcWmsDataSource(jdbc_name: String, lock: String) = {

    if (ds_gc_wms == null) {
      lock.synchronized {
        if (ds_gc_wms == null) {
          ds_gc_wms = new ComboPooledDataSource(jdbc_name);
        }
      }
    }

    ds_gc_wms
  }

  def getZt75StreamingDataSource(jdbc_name: String, lock: String) = {

    if (ds_zt75_streaming == null) {
      lock.synchronized {
        if (ds_zt75_streaming == null) {
          ds_zt75_streaming = new ComboPooledDataSource(jdbc_name);
        }
      }
    }

    ds_zt75_streaming
  }


  /**
    * 关闭连接
    * @param resultSet
    * @param pst
    * @param connection
    */
  def close(resultSet : ResultSet,  pst: Statement, connection: Connection) {

    if (resultSet != null) {
      try {
        resultSet.close();
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }

    if (pst != null) {
      try {
        pst.close();
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }

    if (connection != null) {
      try {
        connection.close();
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }


}
