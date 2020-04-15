package com.zongteng.ztetl.api

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  *
  * HBASE  连接接口
  * @author panmingwen
  *
  */
object HBaseConfig {

  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "192.168.109.73:2181,192.168.109.74:2181,192.168.109.75:2181")

  var connection: Connection = null

  def getConnection(): Connection = {

    if ((connection == null || connection.isClosed()) && conf != null) {
      try {
        connection = ConnectionFactory.createConnection(conf)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    return connection;
  }

  def colse() = {
    if (connection != null) {
      try {
        connection.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
