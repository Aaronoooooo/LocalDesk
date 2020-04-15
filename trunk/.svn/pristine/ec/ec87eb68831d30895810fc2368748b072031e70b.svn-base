package com.zongteng.ztetl.test.spark.stream.util

import java.sql.{Connection, PreparedStatement}

import com.zongteng.ztetl.api.SparkKafkaCon
import com.zongteng.ztetl.common.jdbc.JDBCUtil
import com.zongteng.ztetl.entity.common.InsertModel
import com.zongteng.ztetl.etl.stream.Stream_json_util
import com.zongteng.ztetl.etl.stream.gc_wms_orders.fact.Orders_stream_allow_mysql_table
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

object Orders_stream_code_util2 {

  /**
    * 生成实时导入ods层代码的工具类
    *
    * @param appName        作业名称
    * @param interval       消费时间间隔
    * @param topic_name     kafka中topic名称
    * @param group_Id       消费组
    * @param mysql_database mysql数据库名称
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
        println("isEmpty == "  + rdd.isEmpty())
        if (!rdd.isEmpty()) {

          // 获取对应kafka消费的偏移量
          val offset: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // filter作用：只消费指定数据库指定表的数据
          rdd.filter((e: ConsumerRecord[String, String]) => checkJsonStr(mysql_database, e.value(), datasource_name)).foreachPartition((cols: Iterator[ConsumerRecord[String, String]]) => {

            var connection: Connection = JDBCUtil.getConnection("zongteng72")

            connection.setAutoCommit(false)
            var delstatement: PreparedStatement = null
            var insstatement: PreparedStatement = null
            try {

              var i: Int = 1
              cols.foreach((e: ConsumerRecord[String, String]) => {

                println("connection ==== " + connection)
                delstatement = connection.prepareStatement("DELETE FROM `test`.people WHERE id = ?")
                insstatement = connection.prepareStatement("INSERT INTO `test`.people VALUES(?, ?, ?)")

                delstatement.setInt(1, i)

                insstatement.setInt(1, i)
                insstatement.setString(2, "zongteng")
                insstatement.setInt(3, i)
                i += 1

                delstatement.executeUpdate()
                insstatement.executeUpdate()

                connection.commit()
              })

            } catch {
              case e: Exception => {
                connection.rollback() // 回滚
                throw e
              }
            } finally {
              JDBCUtil.close(null, delstatement, connection)
              JDBCUtil.close(null, insstatement, connection)
            }

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

    val allow_mysql_table: Array[String] = Orders_stream_allow_mysql_table.table
    var result = false

    if (StringUtils.isNotBlank(jsonStr)) {
      val model: InsertModel = Stream_json_util.getInsertObject(jsonStr)
      result = mysql_database.equalsIgnoreCase(model.database) &&
        allow_mysql_table.contains(datasource_name + "_" + model.table)
      println(datasource_name + "_" + model.table + "  ===  " + jsonStr)
    }

    true
  }

}
