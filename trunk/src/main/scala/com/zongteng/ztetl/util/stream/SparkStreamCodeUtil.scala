package com.zongteng.ztetl.util.stream

import java.util

import com.zongteng.ztetl.api.{HBaseConfig, SparkKafkaCon}
import com.zongteng.ztetl.entity.common.InsertModel
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

object SparkStreamCodeUtil {


  /**
    * 生成实时导入ods层代码的工具类
    *
    * @param appName         作业名称
    * @param interval        消费时间间隔
    * @param topic_name      kafka中topic名称
    * @param group_Id        消费组
    * @param mysql_database  mysql数据库名称
    * @param datasource_name 数据来源简称（例如：gc_oms）
    */
  def getRunCode(appName: String, interval: Int, topic_name: String, group_Id: String, mysql_database: String, datasource_name: String) = {

    // 1、获取streaming对象
    val sparkStreamAndKafkaStream = SparkKafkaCon.getConnectKafka(appName, interval, topic_name, group_Id)
    val sparkStream = sparkStreamAndKafkaStream._1
    val kafkaStream = sparkStreamAndKafkaStream._2


    // 2、记录日志开始
    //val task: String = Log.start(appName)

    try {

      kafkaStream.foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) => {

        println(rdd.isEmpty())

        if (!rdd.isEmpty()) {

          // 获取对应kafka消费的偏移量
          val offset: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // filter作用：只消费指定数据库指定表的数据
          rdd.filter((e: ConsumerRecord[String, String]) => checkJsonStr(mysql_database, e.value(), datasource_name)).foreach((e: ConsumerRecord[String, String]) => {

            // hbase连接（必须在这里定义，否则会遇到序列化的问题）
            val connection: Connection = HBaseConfig.getConnection()

            println(e.value())

            // 获取对应的row数据
            val model: InsertModel = SparkJsonUtil.getInsertObject(e.value())
            val puts: util.ArrayList[Put] = SparkJsonUtil.getHBasePut(model)

            // 数据添加到hbase中
            // val table: Table = connection.getTable(TableName.valueOf(ods_table))

            var ods_table: String = null
            if ("gc_inventory_batch_log".equalsIgnoreCase(model.database)) {

              ods_table = datasource_name + "_" + model.table.substring(0, 19)
              println("inventory_batch_log获取hbase表名")

            } else {

              ods_table = datasource_name + "_" + model.table

            }

            val table: Table = connection.getTable(TableName.valueOf(ods_table))

            for (i <- 0 until puts.size) {
              val put: Put = puts.get(i)
              table.put(put)
              println(ods_table + " 添加成功 " + put)
            }
            table.close()
          })

          // 提交对应的kafka消费的偏移量
          kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offset)
        }
      })

      //Log.end(task)
    } catch {
      case e: Exception => e.getMessage
        kafkaStream.stop()
      //Log.end(task)
    }

    sparkStream.start()
    sparkStream.awaitTermination()
  }

  def checkJsonStr(mysql_database: String, jsonStr: String, datasource_name: String) = {

    val allow_mysql_table: Array[String] = AllowMysqlTable.table

    var result = false
    var model: InsertModel = null

    // println(mysql_database + "  ===  " + jsonStr)
    if (StringUtils.isNotBlank(jsonStr)) {
      //如果是日志表就不用判断表是否在AllowMysqlTable类中存在了
      if ("gc_inventory_batch_log".equalsIgnoreCase(mysql_database)) {
        model = SparkJsonUtil.getInsertObject(jsonStr)
        result = mysql_database.equalsIgnoreCase(model.database) &&
          model.table.contains("inventory_batch_log")
      } else {
        model = SparkJsonUtil.getInsertObject(jsonStr)
        result = mysql_database.equalsIgnoreCase(model.database) &&
          allow_mysql_table.contains(datasource_name + "_" + model.table)
      }

    }

    result
  }


}
