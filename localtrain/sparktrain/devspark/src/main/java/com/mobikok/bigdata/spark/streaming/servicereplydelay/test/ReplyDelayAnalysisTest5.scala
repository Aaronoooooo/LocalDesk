package com.mobikok.bigdata.spark.streaming.servicereplydelay.test

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.mobikok.bigdata.spark.streaming.servicereplydelay.util.{JDBCUtil, KafkaConsumerUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

object ReplyDelayAnalysisTest5 {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(ReplyDelayAnalysisTest5.getClass)
    val conf = new SparkConf().setMaster("local[*]").setAppName("chat-records")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = "chat-records"
    val groupId = "g7"
    val kafkaDStream = KafkaConsumerUtil.getKafkaStream(topics, ssc, groupId)

    //读取offset
    //        var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    //        val inputGetoffsetDstream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform { rdd =>
    //          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //          rdd
    //        }
    //______________________________________________________________________________________________________________
    val sdfDelay = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dayFormat = new SimpleDateFormat("yyyy-MM-dd")
    var replyDelayTime: Long = 0
    var replyTime: Long = 0
    var sendTime: String = null
    var firstSendTime: Date = null
    var dateState: String = null
    var todayOverDate: Date = null

    kafkaDStream.foreachRDD { rdd => {
      val offsetRanges = rdd
        .asInstanceOf[HasOffsetRanges]
        .offsetRanges
        .map { x =>
          OffsetRange(x.topic, x.partition, x.fromOffset, x.untilOffset)
        }
      for (offset <- offsetRanges) {
        val partition: Int = offset.partition
        val fromOffset: Long = offset.fromOffset
        val untilOffset: Long = offset.untilOffset
        logger.warn("写入分区：" + partition + ":" + fromOffset + "-->" + untilOffset)
      }
      rdd.foreachPartition {
        jsonObjData => {
          val jsonList: List[JSONObject] = jsonObjData.map(record => {
            JSON.parseObject(record.value())
          }).toList
          val connection: Connection = JDBCUtil.getConnection()
          val jedis: Jedis = RedisUtil.getJedisClient

          for (jsonObj <- jsonList) {
            val messageBody: MessageBodyTest6 = JSON.parseObject(JSON.toJSONString(jsonObj, SerializerFeature.PrettyFormat), classOf[MessageBodyTest6])
            val suId = messageBody.service_user_id
            val usId = messageBody.user_service_id
            val user = messageBody.user
            val chatService = messageBody.chatService
            sendTime = messageBody.time
            val sendDate = sdfDelay.parse(sendTime)
            todayOverDate = sdfDelay.parse(dayFormat.format(sendDate) + " " + "23:59:59")

            //            val sendDate = sdfDelay.parse(sendTime)
            //            todayOverDate = sdfDelay.parse(dayFormat.format(sendDate) + " " + "23:59:59")
            //val dateState: String = sendTime.substring(0, 10).replace(' ', '-')
            dateState = sendTime.substring(0, 10)
            if (usId.nonEmpty) { //用户发送消息存入redis
              //          val userKey = user + ":" + chatService + ":" + usId
              val userKey = dateState + "_" + usId + "_" + user + "_" + chatService
              //存入redis
              //如果未存在则保存 返回1  如果已经存在则不保存 返回0
              jedis.sadd(userKey, sendTime)
              logger.warn("用户" + user + "消息[" + messageBody + "]已存入Redis")
              logger.warn("用户发送时间[" + sendTime + "]")

              val expiresTime = (todayOverDate.getTime - sendDate.getTime) / 1000
              logger.warn("过期时间[" + expiresTime + "]")
              jedis.expire(userKey, expiresTime.toInt)
            } else { //表示为客服回复消息,需按条件存入mysql
              //1.获取接收suId
              //2.按条件存入mysql
              //2.1 查询redis结果作为条件存入
              val key = dateState + "_" + suId + "_" + user + "_" + chatService

              //校验该客服的用户发送消息是否以存在
              if (jedis.exists(key)) {
                logger.warn("客服" + chatService + "回复消息[" + jsonObj.toString + "]")

                //获取客服发送用户数据
                val valueTimeSet: util.Set[String] = jedis.smembers(key)
                //val sremKey = jedis.srem(key,dateState) 删除指定value的key
                val delKey = jedis.del(key)
                logger.warn("Redis中[" + key + ":" + delKey + "]数据已删除")

                //用户连续发送消息时取第一条记录
                if (valueTimeSet.size() == 1) {
                  firstSendTime = sdfDelay.parse(valueTimeSet.toList.head)
                } else if (valueTimeSet.size() > 1) {
                  firstSendTime = sdfDelay.parse(valueTimeSet.toList.sortBy(str => {
                    str
                  }).head)
                }
                replyDelayTime = (todayOverDate.getTime - firstSendTime.getTime) / (1000 * 60)
                logger.warn("回复延迟时间[" + replyDelayTime + "]Minute")
                //按条件存入mysql
                logger.warn("客服回复消息:" + messageBody + "存入mysql")
                ReplyRangeServiceTest6.ReplyRange(replyDelayTime, connection, messageBody)
              } else { //用户未发送消息,客服主动发送
                logger.warn("客服主动发送")
              }
              logger.warn("mysql中添加客服回复延迟记录")
            }
          }
          jedis.close()
          connection.close()
          //          jsonList.toIterator
        }



      }
    }
    }

    // TODO: 清理每天的未回复客服
    //*****************************定时器每天00:00:00执行Start************************************
    val oneDay: Long = 24 * 60 * 60 * 1000
    //定时任务执行时间
    val ExecuteTime = dayFormat.format(new java.util.Date()) + " " + "16:14:15"
    logger.warn("定时任务执行时间:" + ExecuteTime)
    val currentDate = sdfDelay.parse(ExecuteTime)
    var initDelay = currentDate.getTime - new java.util.Date().getTime
    if (initDelay > 0) initDelay else initDelay += oneDay
    logger.warn("定时任务初始化开始剩余时间: " + initDelay / (1000 * 60) + "分钟")
    //          new Runnable {
    //            override def run() = {
    //              val connection: Connection = JDBCUtil.getConnection()
    //              val jedis: Jedis = RedisUtil.getJedisClient
    //              val keys: util.Set[String] = jedis.keys("*")
    //              println("keys *: " + keys)
    //              if (keys.size() > 0) {
    //                for (key <- keys) {
    //                  logger.warn(new java.util.Date() + "查询结果:" + key)
    //                  if (key.contains("_")) {
    //                    val valueTimeSet: util.Set[String] = jedis.smembers(key)
    //                    //                  jedis.del(key)
    //                    println("jedis.smembers: " + valueTimeSet)
    //                    val chatBody: Array[String] = key.split("_")
    //                    if (valueTimeSet.size() == 1) {
    //                      sendTime = valueTimeSet.toList.head
    //                      firstSendTime = sdfDelay.parse(sendTime)
    //                    } else if (valueTimeSet.size() > 1) {
    //                      sendTime = valueTimeSet.toList.sortBy(str => {
    //                        str
    //                      }).head
    //                      firstSendTime = sdfDelay.parse(sendTime)
    //                    }
    //                    println("第一条发送时间:" + sendTime)
    //                    replyDelayTime = (currentDate.getTime - firstSendTime.getTime) / (1000 * 60)
    //                    println("延迟时间: " + replyDelayTime)
    //                    if (replyDelayTime > 0) {
    //                      val body = MessageBodyTest6(chatBody(1), chatBody(1), chatBody(2), chatBody(3), sendTime, replyDelayTime)
    //                      println("客服一直未回复消息:" + body + "存入mysql")
    //                      //按条件存入mysql
    //                      ReplyRangeServiceTest7.ReplyRange(replyDelayTime, connection, body)
    //                    }
    //                    jedis.del(key)
    //                  }
    //                }
    //              }
    //              jedis.close()
    //              connection.close()
    //            }
    //          }
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable {
      override def run() = {
        val connection: Connection = JDBCUtil.getConnection()
        val jedis: Jedis = RedisUtil.getJedisClient
        val keys: util.Set[String] = jedis.keys("*")
        println("keys *: " + keys)
        if (keys.size() > 0) {
          for (key <- keys) {
            logger.warn(new java.util.Date() + "查询结果:" + key)
            if (key.contains("_")) {
              val valueTimeSet: util.Set[String] = jedis.smembers(key)
              //                  jedis.del(key)
              println("jedis.smembers: " + valueTimeSet)
              val chatBody: Array[String] = key.split("_")
              if (valueTimeSet.size() == 1) {
                sendTime = valueTimeSet.toList.head
                firstSendTime = sdfDelay.parse(sendTime)
              } else if (valueTimeSet.size() > 1) {
                sendTime = valueTimeSet.toList.sortBy(str => {
                  str
                }).head
                firstSendTime = sdfDelay.parse(sendTime)
              }
              println("第一条发送时间:" + sendTime)
              replyDelayTime = (currentDate.getTime - firstSendTime.getTime) / (1000 * 60)
              println("延迟时间: " + replyDelayTime)
              if (replyDelayTime > 0) {
                val body = MessageBodyTest6(chatBody(1), chatBody(1), chatBody(2), chatBody(3), sendTime)
                println("客服一直未回复消息:" + body + "存入mysql")
                //按条件存入mysql
                ReplyRangeServiceTest6.ReplyRange(replyDelayTime, connection, body)
              }
              jedis.del(key)
            }
          }
        }
        jedis.close()
        connection.close()
      }
    }, initDelay, oneDay, TimeUnit.MILLISECONDS)
    //              Executors.newScheduledThreadPool(3).scheduleAtFixedRate(runnable, initDelay, oneDay, TimeUnit.MILLISECONDS)
    //*****************************定时器每天00:00:00执行End************************************

    ssc.start()
    ssc.awaitTermination()
  }
}
