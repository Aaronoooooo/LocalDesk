package com.atguigu.gmall.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import net.minidev.json.JSONObject

import scala.collection.mutable.ListBuffer

object PhoenixUtilTest {

  def queryList(sql: String) = {
    val rsList = new ListBuffer[JSONObject]
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181")
    val ps: PreparedStatement = conn.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    while (rs.next()) {
      val userStatusJsonObj = new JSONObject()
      for (i <- 1 to rsMetaData.getColumnCount){
        userStatusJsonObj.put(rsMetaData.getColumnName(i),rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }
    rs.close()
    ps.close()
    conn.clearWarnings()
    rsList.toList
  }

  def main(args: Array[String]): Unit = {
    queryList("select * from user_info")
  }
}