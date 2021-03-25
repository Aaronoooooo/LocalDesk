package com.mobikok.bigdata.spark.streaming.servicereplydelay.service

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.mobikok.bigdata.spark.streaming.servicereplydelay.bean.MessageBody

object ReplyRangeService {


  def ReplyRange(replyDelayTime: Long, connection: Connection, messageBody: MessageBody, userSendTime: String) = {

    var replyDelayRange1_2: Int = 0
    var replyDelayRange2_3: Int = 0
    var replyDelayRange3_6: Int = 0
    var replyDelayRange_6: Int = 0
    val userId = messageBody.userId
    val userName = messageBody.userName
    val fanId = messageBody.fanId
    val fanName = messageBody.fanName
    val serviceSendTime = messageBody.timestamp
    val pageId = messageBody.pageId

    if (Range(10, 20).contains(replyDelayTime)) {
      replyDelayRange1_2 += 1
      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,fans,fansId,chatService,chatServiceId,replyDelayTime,replyDelayRange1_2,serviceSendTime,userSendTime)
          |values (?,?,?,?,?,?,?,?,?)
          |""".stripMargin)
      pstat.setString(1, pageId)
      pstat.setString(2, fanName)
      pstat.setString(3, fanId)
      pstat.setString(4, userName)
      pstat.setString(5, userId)
      pstat.setLong(6, replyDelayTime)
      pstat.setLong(7, replyDelayRange1_2)
      pstat.setString(8, serviceSendTime)
      pstat.setString(9, userSendTime)
      pstat.executeUpdate()
      pstat.close()
      println("正在插入数据: " + messageBody)
      //                    connection.close()
    } else if (Range(20, 30).contains(replyDelayTime)) {
      val pstat0: PreparedStatement = connection.prepareStatement(
        s"""
           |select replyDelayRange1_2 from service_reply_delay where pageId = ?
           |""".stripMargin)
      pstat0.setString(1, pageId)
      val rs: ResultSet = pstat0.executeQuery()
      while (rs.next()) {
        var delayRange: Int = rs.getInt(1)
        if (delayRange > 0) delayRange -= 1
        replyDelayRange1_2 += delayRange
      }
      replyDelayRange2_3 += 1

      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,fans,fansId,chatService,chatServiceId,replyDelayTime,replyDelayRange1_2,replyDelayRange2_3,serviceSendTime,userSendTime)
          |values (?,?,?,?,?,?,?,?,?,?)
          |""".stripMargin)
      pstat.setString(1, pageId)
      pstat.setString(2, fanName)
      pstat.setString(3, fanId)
      pstat.setString(4, userName)
      pstat.setString(5, userId)
      pstat.setLong(6, replyDelayTime)
      pstat.setLong(7, replyDelayRange1_2)
      pstat.setLong(8, replyDelayRange2_3)
      pstat.setString(9, serviceSendTime)
      pstat.setString(10, userSendTime)
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
      pstat0.setString(1, pageId)
      val rs: ResultSet = pstat0.executeQuery()
      while (rs.next()) {
        var delayRange: Int = rs.getInt(1)
        if (delayRange > 0) delayRange -= 1
        replyDelayRange2_3 += delayRange
      }
      replyDelayRange3_6 += 1
      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,fans,fansId,chatService,chatServiceId,replyDelayTime,replyDelayRange2_3,replyDelayRange3_6,serviceSendTime,userSendTime)
          |values (?,?,?,?,?,?,?,?,?,?)
          |""".stripMargin)
      pstat.setString(1, pageId)
      pstat.setString(2, fanName)
      pstat.setString(3, fanId)
      pstat.setString(4, userName)
      pstat.setString(5, userId)
      pstat.setLong(6, replyDelayTime)
      pstat.setLong(7, replyDelayRange2_3)
      pstat.setLong(8, replyDelayRange3_6)
      pstat.setString(9, serviceSendTime)
      pstat.setString(10, userSendTime)
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
      pstat0.setString(1, pageId)
      val rs: ResultSet = pstat0.executeQuery()
      while (rs.next()) {
        var delayRange: Int = rs.getInt(1)
        if (delayRange > 0) delayRange -= 1
        replyDelayRange3_6 += delayRange
      }
      replyDelayRange_6 += 1
      val pstat: PreparedStatement = connection.prepareStatement(
        """
          |insert into service_reply_delay (pageId,fans,fansId,chatService,chatServiceId,replyDelayTime,replyDelayRange3_6,replyDelayRange_6,serviceSendTime,userSendTime)
          |values (?,?,?,?,?,?,?,?,?,?)
          |""".stripMargin)
      pstat.setString(1, pageId)
      pstat.setString(2, fanName)
      pstat.setString(3, fanId)
      pstat.setString(4, userName)
      pstat.setString(5, userId)
      pstat.setLong(6, replyDelayTime)
      pstat.setLong(7, replyDelayRange3_6)
      pstat.setLong(8, replyDelayRange_6)
      pstat.setString(9, serviceSendTime)
      pstat.setString(10, userSendTime)
      pstat.executeUpdate()
      pstat0.close()
      pstat.close()
      println("正在插入数据: " + messageBody)
    }
    //    connection.close()
  }
}
