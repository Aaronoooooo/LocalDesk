package com.mobikok.bigdata.spark.streaming.servicereplydelay.bean

case class MessageBody(
                        pageId: String,
                        senderId: String,
                        recipientId: String,
                        fanId: String,
                        fanName: String,
                        userId: String,
                        userName: String,
                        timestamp: String //发送时间
                      )

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
