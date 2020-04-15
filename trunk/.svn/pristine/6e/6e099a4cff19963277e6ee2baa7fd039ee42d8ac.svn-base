package com.zongteng.ztetl.util

import java.sql.{PreparedStatement, ResultSet}
import com.zongteng.ztetl.util.Log.{connect, errorMesage, con,closeConnection}

object EtlIncLog {
  /**
    *
    * 获取实时表上一次成功时间  将任务编号传入进来
    *
    */
  def GetLastSuccessTime(task: String): String = {
    var ps: PreparedStatement = null;
    var resultSet: ResultSet = null
    try {
      if (con == null)
        connect();
      val sql = "select end_time from etl_inc_log  where id  in (SELECT max(id) as id from etl_inc_log  where status='SUCCESS' and task= ? )"
      ps = con.prepareStatement(sql);
      ps.setString(1, task)
      resultSet = ps.executeQuery()
      //指针向下移动一位
      resultSet.next()
      val last_success_time: String = resultSet.getString("end_time")
      resultSet.close();
      ps.close();
      con.close();
      return last_success_time;
    } catch {
      case ex: Exception => errorMesage(ex);

    }
    finally {
      closeConnection(con,ps,resultSet)
    }
    return "-1"
  }

  /**
    *
    * 获取实时表上一次成功时间  将任务编号传入进来
    *
    */
  def GetStartEndTime(id: String): String = {
    var ps: PreparedStatement = null;
    var resultSet: ResultSet = null
    try {
      if (con == null)
        connect();
      val sql = "select start_time,end_time from etl_inc_log  where id = ?"
      ps = con.prepareStatement(sql);
      ps.setString(1, id)
      resultSet = ps.executeQuery()
      //指针向下移动一位
      resultSet.next()
      val start_time: String = resultSet.getString("start_time")
      val end_time: String = resultSet.getString("end_time")
      resultSet.close();
      ps.close();
      con.close();
      return start_time + "," + end_time;
    } catch {
      case ex: Exception => errorMesage(ex);

    }
    finally {
      closeConnection(con,ps,resultSet)
    }
    return "-1"
  }

  /**
    *
    * 添加事实表增量调度记录,提供任务编号,往前冗余的时间(天)
    *
    */
  def start(task: String, dayNum: Int): String = {
    var ps: PreparedStatement = null;
    try {
      //获取当前时间作为id
      val id: String = DateUtil.getNowTimess()
      //获取之前成功的最大时间
      val last_success_time: String = GetLastSuccessTime(task)
      if (con == null)
        connect();
      val sql = "insert into etl_inc_log(id,task,start_time,end_time,status,task_start_time) " +
        " values(?,?,FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(cast( ? as date),INTERVAL  ?  day)))" +
        " ,FROM_UNIXTIME(UNIX_TIMESTAMP(cast(now() as date))-1),'running',CURRENT_TIMESTAMP()) "
      ps = con.prepareStatement(sql);
      ps.setString(1, id);
      ps.setString(2, task);
      ps.setString(3, last_success_time);
      ps.setInt(4, dayNum - 1);
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
    return "-1"
  }


  /**
    * 当前任务事实表执行完毕,修改完成时间
    */

  def end(id: String) {
    var ps: PreparedStatement = null;
    try {
      if (con == null)
        connect();
      ps = con.prepareStatement("UPDATE etl_inc_log SET status='SUCCESS',start_time=start_time,end_time=end_time,task_start_time=task_start_time,task_end_time = CURRENT_TIMESTAMP(),during =TIMESTAMPDIFF(SECOND,task_start_time, CURRENT_TIMESTAMP()) WHERE id = ?");
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
        connect();
      ps = con.prepareStatement("UPDATE etl_log SET status='FAILED',start_time=start_time,end_time=end_time,task_start_time=task_start_time,task_end_time = CURRENT_TIMESTAMP(),during =TIMESTAMPDIFF(SECOND,task_start_time, CURRENT_TIMESTAMP()), error_msg = ?  WHERE id = ?");
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
