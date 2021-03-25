package com.mobikok.bigdata.spark.streaming.servicereplydelay.test

import java.sql.{Connection, PreparedStatement, ResultSet}

object ReplyRangeServiceTest6 {


  def ReplyRange(replyDelayTime: Long, connection: Connection, messageBody: MessageBodyTest6) = {

    var replyDelayRange1_2: Int = 0
    var replyDelayRange2_3: Int = 0
    var replyDelayRange3_6: Int = 0
    var replyDelayRange_6: Int = 0
    val suId = messageBody.service_user_id
    val user = messageBody.user
    val chatService = messageBody.chatService
    val time = messageBody.time

    if (Range(10, 20).contains(replyDelayTime)) {
      replyDelayRange1_2 += 1
      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange1_2,chatTime)
          |values (?,?,?,?,?,?)
          |on duplicate key
          |update replyDelayTime = ?,replyDelayRange1_2 = replyDelayRange1_2 + ?
          |""".stripMargin)
      pstat.setString(1, suId)
      pstat.setString(2, user)
      pstat.setString(3, chatService)
      pstat.setLong(4, replyDelayTime)
      pstat.setLong(5, replyDelayRange1_2)
      pstat.setString(6, time)
      pstat.setLong(7, replyDelayTime)
      pstat.setLong(8, replyDelayRange1_2)
      pstat.executeUpdate()
      pstat.close()
      println("正在插入数据: " + messageBody)
      //                    connection.close()
    } else if (Range(20, 30).contains(replyDelayTime)) {
      val pstat0: PreparedStatement = connection.prepareStatement(
        s"""
           |select replyDelayRange1_2 from service_reply_delay where pageId = ?
           |""".stripMargin)
      pstat0.setString(1, suId)
      val rs: ResultSet = pstat0.executeQuery()
      while (rs.next()) {
        var delayRange: Int = rs.getInt(1)
        if (delayRange > 0) delayRange -= 1
        replyDelayRange1_2 += delayRange
      }
      replyDelayRange2_3 += 1

      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange1_2,replyDelayRange2_3,chatTime)
          |values (?,?,?,?,?,?,?)
          |on duplicate key
          |update replyDelayTime = ?,replyDelayRange1_2 = ?,replyDelayRange2_3 = replyDelayRange2_3 + ?
          |""".stripMargin)
      pstat.setString(1, suId)
      pstat.setString(2, user)
      pstat.setString(3, chatService)
      pstat.setLong(4, replyDelayTime)
      pstat.setLong(5, replyDelayRange1_2)
      pstat.setLong(6, replyDelayRange2_3)
      pstat.setString(7, time)
      pstat.setLong(8, replyDelayTime)
      pstat.setLong(9, replyDelayRange1_2)
      pstat.setLong(10, replyDelayRange2_3)
      pstat.executeUpdate()
      pstat0.close()
      pstat.close()
      println("正在插入数据: " + messageBody)
      //                    connection.close()
    } else if (Range(30, 60).contains(replyDelayTime)) {
      val pstat0: PreparedStatement = connection.prepareStatement(
        s"""
           |select replyDelayRange2_3 from service_reply_delay where pageId = ?
           |""".stripMargin)
      pstat0.setString(1, suId)
      val rs: ResultSet = pstat0.executeQuery()
      while (rs.next()) {
        var delayRange: Int = rs.getInt(1)
        if (delayRange > 0) delayRange -= 1
        replyDelayRange2_3 += delayRange
      }
      replyDelayRange3_6 += 1
      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange2_3,replyDelayRange3_6,chatTime)
          |values (?,?,?,?,?,?,?)
          |on duplicate key
          |update replyDelayTime = ?,replyDelayRange2_3 = ?,replyDelayRange3_6 = replyDelayRange3_6 + ?
          |""".stripMargin)
      pstat.setString(1, suId)
      pstat.setString(2, user)
      pstat.setString(3, chatService)
      pstat.setLong(4, replyDelayTime)
      pstat.setLong(5, replyDelayRange2_3)
      pstat.setLong(6, replyDelayRange3_6)
      pstat.setString(7, time)
      pstat.setLong(8, replyDelayTime)
      pstat.setLong(9, replyDelayRange1_2)
      pstat.setLong(10, replyDelayRange2_3)
      pstat.executeUpdate()
      pstat0.close()
      pstat.close()
      println("正在插入数据: " + messageBody)
      //                    connection.close()
    } else if (replyDelayTime >= 60) {
      val pstat0: PreparedStatement = connection.prepareStatement(
        s"""
           |select replyDelayRange3_6 from service_reply_delay where pageId = ?
           |""".stripMargin)
      pstat0.setString(1, suId)
      val rs: ResultSet = pstat0.executeQuery()
      while (rs.next()) {
        var delayRange: Int = rs.getInt(1)
        if (delayRange > 0) delayRange -= 1
        replyDelayRange3_6 += delayRange
      }
      replyDelayRange_6 += 1
      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange3_6,replyDelayRange_6,chatTime)
          |values (?,?,?,?,?,?,?)
          |on duplicate key
          |update replyDelayTime = ?,replyDelayRange3_6 = ?,replyDelayRange_6 = replyDelayRange_6 + ?
          |""".stripMargin)
      pstat.setString(1, suId)
      pstat.setString(2, user)
      pstat.setString(3, chatService)
      pstat.setLong(4, replyDelayTime)
      pstat.setLong(5, replyDelayRange3_6)
      pstat.setLong(6, replyDelayRange_6)
      pstat.setString(7, time)
      pstat.setLong(8, replyDelayTime)
      pstat.setLong(9, replyDelayRange1_2)
      pstat.setLong(10, replyDelayRange2_3)
      pstat.executeUpdate()
      pstat0.close()
      pstat.close()
      println("正在插入数据: " + messageBody)
    }
//    connection.close()
  }
}
