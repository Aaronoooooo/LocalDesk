package com.zongteng.ztetl.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}


/**
  *
  * 日志接口
  *
  * @author panmingwen
  *
  */
object Log {


  var con: Connection = null;
  /**
    *
    * 创建连接
    *
    */
  def connect() {
    try {
      val driver = PropertyFile.getProperty("driver");
      val url = PropertyFile.getProperty("url");
      val user = PropertyFile.getProperty("user");
      val pwd = PropertyFile.getProperty("pwd");

      Class.forName(driver).newInstance()
      con = DriverManager.getConnection(url, user, pwd)
    } catch {
      case ex: Exception => errorMesage(ex);
    }
  }

  /**
    *
    * 开始日志
    *
    */
  def start(task: String): String = {
    var ps: PreparedStatement = null;
    try {
      val id = task + DateUtil.getNow();
      if (con == null)
        connect()
      val sql = "insert into etl_log(id,task,start_time) values(?,?,CURRENT_TIMESTAMP())"
      ps = con.prepareStatement(sql);
      ps.setString(1, id);
      ps.setString(2, task);
      ps.execute();
      ps.close();
      con.close();
      return id;
    } catch {
      case ex: Exception => errorMesage(ex);
    }
    finally {
      closeConnection(con,ps)
  }
    return "-1";
  }


  def errorMesage(ex: Exception): String = {
    return ex.getMessage;
  }

  /**
    * 关闭连接
    */

  def closeConnection(con: Connection, ps: PreparedStatement) = {
    if (con != null) {
      try {
        con.close()
      } catch {
        case e: Exception => errorMesage(e)
      }
    }

    if (ps != null) {

      try {
        ps.close()
      } catch {
        case e:Exception =>errorMesage(e)
      }
    }
  }

  def closeConnection(con: Connection, ps: PreparedStatement, resultSet: ResultSet) = {

    if (con != null) {
      try {
        con.close()
      } catch {
        case e: Exception => errorMesage(e)
      }
    }

    if (ps != null) {

      try {
        ps.close()
      } catch {
        case e:Exception =>errorMesage(e)
      }
    }
    if (resultSet != null) {
      try {
        resultSet.close()
      } catch {
        case e:Exception =>errorMesage(e)
      }
    }
  }

  /**
    * 日志结束
    */

  def end(id: String) {
    var ps: PreparedStatement = null;
    try {
      if (con == null)
        connect()
      ps = con.prepareStatement("UPDATE etl_log SET end_time = CURRENT_TIMESTAMP(),during = CURRENT_TIMESTAMP()-start_time,is_success = 'Y' WHERE id = ?");
      ps.setString(1, id);
      ps.executeUpdate();
      ps.close();
      con.close();
    } catch {
      case ex: Exception => errorMesage(ex);
    } finally {
     closeConnection(con,ps)
    }
  }


  /**
    * 错误日志
    */
  def error(id: String, error: Exception) {


    var ps: PreparedStatement = null;
    try {
      var message = error.toString() + "\n";
      if (error.getCause() != null) {
        message = message + "Caused by: " + error.getCause().getMessage();
      }
      if (message.length() > 4000) {
        message = message.substring(0, 4000);
      }
      System.out.println("error=" + message);

      if (con == null)
        connect()
      ps = con.prepareStatement("UPDATE etl_log SET end_time = CURRENT_TIMESTAMP(), during = CURRENT_TIMESTAMP()-start_time,is_success = 'N', error_msg = ?  WHERE id = ?");
      ps.setString(1, message);
      ps.setString(2, id);
      ps.executeUpdate();
      ps.close();
      con.close();


    } catch {
      case ex: Exception => errorMesage(ex);
    } finally {
     closeConnection(con,ps)
    }
  }

}