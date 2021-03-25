package com.mobikok.bigdata.spark.streaming.req.bean

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

class KafkaMessage {
  def structType = KafkaMessage.structType
}

object KafkaMessage {

  //
  //  insert overwrite table xxx
  //  select publisherId,subId,offerId, sum(sendPrice), sum(reportPrice)
  //  from dwi where day = 'xxx'
  //  group by concat(appId, countryId) ,publisherId ,subId,offerId

  //  publisherId, sendPrice
  //  11,        ,  12.2
  //  null       ,  11
  //  null       ,  12

  //注意字段名两边不要含空格！！
  val structType: StructType = {
    StructType(Array(
      StructField("user", IntegerType),
      StructField("service", IntegerType),
      StructField("send", IntegerType),
      StructField("time", IntegerType)
    ))
  }
}
