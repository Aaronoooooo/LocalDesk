package com.mobikok.bigdata.spark.streaming.servicereplydelay.test

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{LinkedHashMap => JLinkedHashMap}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.Gson
import com.mobikok.bigdata.spark.streaming.servicereplydelay.util.{JDBCUtil, RedisUtil}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Spark Streaming 处理 kafka json格式数据,并转成DataFrame
 */
object JSONDataHandler {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(JSONDataHandler.getClass)
    val conf = new SparkConf().setMaster("local[*]").setAppName("com.mobikok.bigdata.spark.streaming.rep2.JSONDataHandler")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_streaming",
      //      "auto.offset.reset" -> "earliest",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("chat-records")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetoffsetDstream: DStream[ConsumerRecord[String, String]] = stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val jSONObjectDStream: DStream[JSONObject] = inputGetoffsetDstream.map { record =>
      val jsonString: String = record.value()
      // TODO: 校验数据格式是否合法
      val jSONObject = JSON.parseObject(jsonString)
      jSONObject
    }

    //______________________________________________________________________________________________________________
    jSONObjectDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjData => {
            val jsonList: List[JSONObject] = jsonObjData.toList
            val connection: Connection = JDBCUtil.getConnection()
            val jedis: Jedis = RedisUtil.getJedisClient
            for (jsonObj <- jsonList) {

              val usId: String = jsonObj.getString("user_service_id")
              val suId: String = jsonObj.getString("service_user_id")
              val currentTime: String = jsonObj.getString("time")
              val user: String = jsonObj.getString("user")
              val chatService: String = jsonObj.getString("chatService")
              val sdfDelay = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              //              val dateState: String = currentTime.substring(0, 10).replace(' ', '-')
              val dateState: String = currentTime.substring(0, 10)
              import scala.collection.JavaConversions._

              // TODO: 清理每天的未回复客服
              val overTime: String = DateFormatUtils.format(new java.util.Date(), "yyyy-MM-dd 23:59:59")
              val sysTime: String = sdfDelay.format(new java.util.Date())
              if (sysTime.equals(overTime)) {
                val keys: util.Set[String] = jedis.keys("*")
                for (key <- keys) {
                  if (key.contains(dateState)) {
                    val value: util.Set[String] = jedis.smembers(key)
                    val chatBody: Array[String] = key.split("_")
                  }
                }
              }

              if (usId.nonEmpty) { //用户发送消息存入redis
                //          val userKey = user + ":" + chatService + ":" + usId
                val userKey = dateState + "_" + usId + "_" + user + "_" + chatService
                //存入redis
                //如果未存在则保存 返回1  如果已经存在则不保存 返回0
                jedis.sadd(userKey, currentTime)
                logger.warn("用户" + user + "消息[" + jsonObj.toString + "]已存入Redis")

                //                val overTime: String = DateFormatUtils.format(sdfDelay.parse(currentTime), "yyyy-MM-dd 23:59:59")
                logger.warn("当前聊天时间[" + currentTime + "],当天结束时间[" + overTime + "]")

                val expiresTime = (sdfDelay.parse(overTime).getTime - sdfDelay.parse(currentTime).getTime) / 1000
                logger.warn("过期时间[" + expiresTime + "]")
                jedis.expire(userKey, expiresTime.toInt)
              } else { //表示为客服回复消息,需按条件存入mysql
                //1.获取接收suId
                //2.按条件存入mysql
                //2.1 查询redis结果作为条件存入
                val key = dateState + "_" + suId + "_" + user + "_" + chatService
                //suId不为空,表示客服回复
                if (jedis.exists(key)) {
                  var replyDelayRange1_2: Int = 0
                  var replyDelayRange2_3: Int = 0
                  var replyDelayRange3_6: Int = 0
                  var replyDelayRange_6: Int = 0
                  logger.warn("客服" + chatService + "回复消息[" + jsonObj.toString + "]")
                  val sortList = new ListBuffer[String]()
                  //获取客服发送用户数据
                  val valueTimeSet: util.Set[String] = jedis.smembers(key)
                  //val sremKey = jedis.srem(key,dateState) 删除指定value的key
                  val delKey = jedis.del(key)
                  logger.warn("Redis中[" + key + "]数据已删除")

                  var replyDelayTime: Long = 0
                  var replyTime: Long = 0
                  var sendTime: Long = 0

                  //用户连续发送消息时取第一条记录
                  if (valueTimeSet.nonEmpty && valueTimeSet.size() < 1) {
                    for (onlyTime <- valueTimeSet) {
                      sendTime = sdfDelay.parse(onlyTime).getTime
                      replyTime = sdfDelay.parse(currentTime).getTime
                    }
                  } else if (valueTimeSet.nonEmpty && valueTimeSet.size() > 0) {
                    for (valueTime <- valueTimeSet) {
                      sortList += valueTime
                    }
                    val firstSendTime: String = sortList.sortBy(str => {
                      str
                    }).head
                    sendTime = sdfDelay.parse(firstSendTime).getTime
                    replyTime = sdfDelay.parse(currentTime).getTime
                  }

                  //
                  replyDelayTime = (replyTime - sendTime) / (1000 * 60)
                  logger.warn("回复延迟时间[" + replyDelayTime + "]Minute")
                  //
                  if (Range(10, 20).contains(replyDelayTime)) {
                    replyDelayRange1_2 += 1
                    val pstat: PreparedStatement = connection.prepareStatement(
                      """
                        |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange1_2)
                        |values (?,?,?,?,?)
                        |on duplicate key
                        |update replyDelayTime = ?,replyDelayRange1_2 = replyDelayRange1_2 + ?
                        |""".stripMargin)
                    pstat.setString(1, suId)
                    pstat.setString(2, user)
                    pstat.setString(3, chatService)
                    pstat.setLong(4, replyDelayTime)
                    pstat.setLong(5, replyDelayRange1_2)
                    pstat.setLong(6, replyDelayTime)
                    pstat.setLong(7, replyDelayRange1_2)
                    pstat.executeUpdate()
                    pstat.close()
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
                        |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange1_2,replyDelayRange2_3)
                        |values (?,?,?,?,?,?)
                        |on duplicate key
                        |update replyDelayTime = ?,replyDelayRange1_2 = ?,replyDelayRange2_3 = replyDelayRange2_3 + ?
                        |""".stripMargin)
                    pstat.setString(1, suId)
                    pstat.setString(2, user)
                    pstat.setString(3, chatService)
                    pstat.setLong(4, replyDelayTime)
                    pstat.setLong(5, replyDelayRange1_2)
                    pstat.setLong(6, replyDelayRange2_3)
                    pstat.setLong(7, replyDelayTime)
                    pstat.setLong(8, replyDelayRange1_2)
                    pstat.setLong(9, replyDelayRange2_3)
                    pstat.executeUpdate()
                    pstat0.close()
                    pstat.close()
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
                        |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange2_3,replyDelayRange3_6)
                        |values (?,?,?,?,?,?)
                        |on duplicate key
                        |update replyDelayTime = ?,replyDelayRange2_3 = ?,replyDelayRange3_6 = replyDelayRange3_6 + ?
                        |""".stripMargin)
                    pstat.setString(1, suId)
                    pstat.setString(2, user)
                    pstat.setString(3, chatService)
                    pstat.setLong(4, replyDelayTime)
                    pstat.setLong(5, replyDelayRange2_3)
                    pstat.setLong(6, replyDelayRange3_6)
                    pstat.setLong(7, replyDelayTime)
                    pstat.setLong(8, replyDelayRange2_3)
                    pstat.setLong(9, replyDelayRange3_6)
                    pstat.executeUpdate()
                    pstat0.close()
                    pstat.close()
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
                        |insert into service_reply_delay (pageId,user,chatService,replyDelayTime,replyDelayRange3_6,replyDelayRange_6)
                        |values (?,?,?,?,?,?)
                        |on duplicate key
                        |update replyDelayTime = ?,replyDelayRange3_6 = ?,replyDelayRange_6 = replyDelayRange_6 + ?
                        |""".stripMargin)
                    pstat.setString(1, suId)
                    pstat.setString(2, user)
                    pstat.setString(3, chatService)
                    pstat.setLong(4, replyDelayTime)
                    pstat.setLong(5, replyDelayRange3_6)
                    pstat.setLong(6, replyDelayRange_6)
                    pstat.setLong(7, replyDelayTime)
                    pstat.setLong(8, replyDelayRange3_6)
                    pstat.setLong(9, replyDelayRange_6)
                    pstat.executeUpdate()
                    pstat0.close()
                    pstat.close()
                  }
                } else { //消息异常,客服主动发送
                  logger.warn("客服主动发送")
                }
                logger.warn("mysql中添加客服回复延迟记录")
              }
            }

            logger.warn("当前时间:" + new java.util.Date())
            jedis.close()
            connection.close()
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}