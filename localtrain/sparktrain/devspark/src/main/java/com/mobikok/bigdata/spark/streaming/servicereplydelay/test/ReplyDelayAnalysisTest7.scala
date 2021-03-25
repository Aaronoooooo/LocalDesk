package com.mobikok.bigdata.spark.streaming.servicereplydelay.test

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}
import java.{lang, util}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.mobikok.bigdata.spark.streaming.servicereplydelay.bean.MessageBody
import com.mobikok.bigdata.spark.streaming.servicereplydelay.util.{JDBCUtil, KafkaConsumerUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

object ReplyDelayAnalysisTest7 {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(ReplyDelayAnalysisTest7.getClass)
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setMaster("local[*]").setAppName("chat-records")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Array[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]]))
    val ssc = new StreamingContext(conf, Seconds(5))
    val sparkSession: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    //    val session: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().appName("sparkSQL").getOrCreate()

    //配置hive支持动态分区
    sparkSession.sql("set hive_exec.dynamic.partition=true")
    sparkSession.sql("set hive_exec.dynamic.partition.mode=nonstrict")
    //    sparkSession.sql("CREATE TABLE person (id int, name string, age int) row format delimited fields terminated by ' '")
    sparkSession.sql(
      """
        |create table if not exists kafkamessage(
        |pageId String,
        |senderId String,
        |recipientId String,
        |fanId String,
        |fanName String,
        |userId String,
        |userName String,
        |sendTime String)
        |row format delimited fields terminated by '\001'
        |""".stripMargin)
    sparkSession.sql("show tables").show()

    val topics = "chat-records"
    val groupId = "g9"
    val kafkaDStream = KafkaConsumerUtil.getKafkaStream(topics, ssc, groupId)

    //    val kafkaStructType: StructType = StructType(Array(
    //      StructField("pageId", StringType),
    //      StructField("senderId", StringType),
    //      StructField("recipientId", StringType),
    //      StructField("fanId", StringType),
    //      StructField("fanName", StringType),
    //      StructField("userId", StringType),
    //      StructField("userName", StringType),
    //      StructField("timestamp", StringType)
    //    ))

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
    var userFirstSendTime: String = null
    var userId: String = null
    var firstSendTime: Date = null
    var dateState: String = null
    var todayOverDate: Date = null

    /*-----------------*/
    //    读取kafka数据插入hive表中
    kafkaDStream.map(record => {
      logger.warn(record.value())
      val body: MessageBody = JSON.parseObject(record.value(), classOf[MessageBody])
      body
    }).foreachRDD(rdd => {
      sparkSession.createDataFrame(rdd).createOrReplaceTempView("temp_table_kafka")
      sparkSession.sql("select * from temp_table_kafka").show()
      //insert
      sparkSession.sql(
        """
          |insert into kafkamessage
          |select
          |pageId,
          |senderId,
          |recipientId,
          |fanId,
          |fanName,
          |userId,
          |userName,
          |timestamp as sendtime
          |from temp_table_kafka
          |""".stripMargin)
      //select
      sparkSession.sql(
        """
          |select
          |pageId,
          |senderId,
          |recipientId,
          |fanId,
          |fanName,
          |userId,
          |userName,
          |sendTime
          |from kafkamessage
          |""".stripMargin).show()
    })
    /*-----------------*/
    kafkaDStream.foreachRDD {
      rdd => {
        val offsetRanges = rdd
          .asInstanceOf[HasOffsetRanges]
          .offsetRanges
          .map { x =>
            OffsetRange(x.topic, x.partition, x.fromOffset, x.untilOffset)
          }
        for (offset <- offsetRanges) {
          logger.warn("写入分区："+offset.partition +":"+offset.fromOffset+"-->"+offset.untilOffset)
        }
        rdd.foreachPartition { jsonObjData => {
          val connection: Connection = JDBCUtil.getConnection()
          val jedis: Jedis = RedisUtil.getJedisClient
          val jsonList: List[JSONObject] = jsonObjData.map(record => {
            JSON.parseObject(record.value())
          }).toList
          for (jsonObj <- jsonList) {
            val messageBody: MessageBody = JSON.parseObject(JSON.toJSONString(jsonObj, SerializerFeature.PrettyFormat), classOf[MessageBody])
            userId = messageBody.userId
            val userName = messageBody.userName
            val senderId = messageBody.senderId
            val recipientId = messageBody.recipientId
            val pageId = messageBody.pageId
            val fanId = messageBody.fanId
            val fanName = messageBody.fanName
            sendTime = messageBody.timestamp
            //            if (!usId.equals(pageId)) serviceSendTime=sendTime
            /*同一聊天窗口:
            pageId和senderId相等
            val usId = messageBody.senderId//粉丝或页面id(senderId=pageId =>页面发送)
            sendTime = messageBody.timestamp//发送时间
            */

            val sendDate = sdfDelay.parse(sendTime)
            todayOverDate = sdfDelay.parse(dayFormat.format(sendDate) + " " + "23:59:59")
            //val dateState: String = sendTime.substring(0, 10).replace(' ', '-')
            dateState = sendTime.substring(0, 10)
            if (!pageId.equals(senderId)) { //粉丝发送消息存入redis
              val userKey = dateState + "_" + pageId + "_" + recipientId + "_" + fanId + "_" + fanName
              //              jedis.sadd(userKey, sendTime)
              jedis.rpush(userKey, sendTime)
              logger.warn("粉丝" + fanId + "消息[" + messageBody + "]已存入Redis")
              logger.warn("粉丝发送时间[" + sendTime + "]")
              val expiresTime = (todayOverDate.getTime - sendDate.getTime) / (1000 * 60)
              logger.warn("过期时间[" + expiresTime + "]Minutes")
              jedis.expire(userKey, expiresTime.toInt)
            } else if (!userId.equals("") && senderId.equals(pageId)) { //表示为客服回复消息,按条件存入mysql
              //1.获取接收suId
              //2.按条件存入mysql
              //2.1 查询redis结果作为条件存入
              /*单分区可保证有序,无需重新排序
              val filteredList=new ListBuffer[String]()
              filteredList+=sendTime+"_"+recipient+"_"+suId
              val firstServiceTime = filteredList.toList.sorted.head
              val serviceTime = firstServiceTime.split("_")
              val key = dateState + "_" + serviceTime(1)*/
              //校验该客服的粉丝发送消息是否以存在
              val serviceKey = dateState + "_" + pageId + "_" + senderId + "_" + fanId + "_" + fanName
              if (jedis.exists(serviceKey)) {
                logger.warn("客服:[" + userId + "]已回复粉丝[" + fanId + "]消息[" + jsonObj.toString + "]")

                //客服回复消息反查用户发送的第一条
                userFirstSendTime = jedis.lindex(serviceKey, 0)
                val long: lang.Long = jedis.del(serviceKey)
                logger.warn("客服已回复,正在删除用户消息[" + long + "]")
                firstSendTime = sdfDelay.parse(userFirstSendTime)
                /*无需使用Set,如用Set类型保存则需重新排序
                val valueTimeSet: util.Set[String] = jedis.smembers(serviceKey)
                //val sremKey = jedis.srem(key,dateState) 删除指定value的key
                val delKey = jedis.del(serviceKey)
                logger.warn("Redis中[" + serviceKey + ":" + delKey + "]数据已删除")

                //用户连续发送消息时取第一条记录
                if (valueTimeSet.size() == 1) {
                  firstSendTime = sdfDelay.parse(valueTimeSet.toList.head)
                } else if (valueTimeSet.size() > 1) {
                  firstSendTime = sdfDelay.parse(valueTimeSet.toList.sortBy(str => {
                    str
                  }).head)
                }*/
                replyDelayTime = (sendDate.getTime - firstSendTime.getTime) / (1000 * 60)
                logger.warn("回复延迟时间[" + replyDelayTime + "]Minutes")
                if (replyDelayTime > 9) {
                  //按条件存入mysql
                  logger.warn("客服回复消息:" + messageBody + "存入mysql")
                  ReplyRangeServiceTest7.ReplyRange(replyDelayTime, connection, messageBody, userFirstSendTime)
                }
              } else logger.warn("客服" + userId + "主动发送消息[" + messageBody + "]")
            } else logger.warn("机器人发送消息[" + messageBody + "]")

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
    val executeTime = dayFormat.format(new java.util.Date()) + " " + "20:48:30"
    logger.warn("定时任务执行时间:" + executeTime)
    val currentDate = sdfDelay.parse(executeTime)
    var initDelay = currentDate.getTime - new java.util.Date().getTime
    if (initDelay > 0) initDelay else initDelay += oneDay
    logger.warn("定时任务初始化开始剩余时间: " + initDelay / (1000 * 60) + "分钟")
    val runnable = new Runnable() {
      override def run() = {
        val connection: Connection = JDBCUtil.getConnection()
        val jedis: Jedis = RedisUtil.getJedisClient
        val keys: util.Set[String] = jedis.keys("*")
        println("keys *: " + keys)
        if (keys.size() > 0) {
          for (key <- keys) {
            logger.warn(new java.util.Date() + "查询结果:" + key)
            if (key.contains("_")) {
              sendTime = jedis.lindex(key, 0)
              firstSendTime = sdfDelay.parse(sendTime)
              // val valueTimeSet: util.Set[String] = jedis.smembers(key)
              // jedis.del(key)
              val chatBody = key.split("_")
              logger.warn("用户Id[" + chatBody(3) + "]->firstSendTime: " + sendTime)
              /*无需使用Set,如用Set类型保存则需重新排序
              if (valueTimeSet.size() == 1) {
                sendTime = valueTimeSet.toList.head
                firstSendTime = sdfDelay.parse(sendTime)
              } else if (valueTimeSet.size() > 1) {
                sendTime = valueTimeSet.toList.sortBy(str => {
                  str
                }).head
                firstSendTime = sdfDelay.parse(sendTime)
              }*/
              replyDelayTime = (currentDate.getTime - firstSendTime.getTime) / (1000 * 60)
              logger.warn("回复延迟时间[" + replyDelayTime + "]Minutes")
              if (replyDelayTime > 9) {
                val body = MessageBody(chatBody(1), chatBody(3), chatBody(2), chatBody(3), chatBody(4), chatBody(2), null, executeTime)
                logger.warn("客服一直未回复消息:[" + body + "]存入mysql")
                //按条件存入mysql
                ReplyRangeServiceTest7.ReplyRange(replyDelayTime, connection, body, sendTime)
              }
              val long: lang.Long = jedis.del(key)
              logger.warn("正在删除未回复消息[" + long + "]")
            }
          }
        }
        jedis.close()
        connection.close()
      }
    }
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(runnable, initDelay, oneDay, TimeUnit.MILLISECONDS)
    //Executors.newScheduledThreadPool(3).scheduleAtFixedRate(runnable, initDelay, oneDay, TimeUnit.MILLISECONDS)
    //*****************************定时器每天00:00:00执行End************************************
    ssc.start()
    ssc.awaitTermination()
  }
}
