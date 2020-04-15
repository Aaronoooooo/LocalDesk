package com.zongteng.ztetl.etl.stream.gc_wms_orders.fact

import java.lang.reflect.Field
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util

import com.google.gson.{Gson, JsonObject, JsonParser}
import com.zongteng.ztetl.api.SparkKafkaCon
import com.zongteng.ztetl.common.jdbc.JDBCUtil
import com.zongteng.ztetl.entity.common.{Sync_order_Insert, Sync_order_date, Sync_order_update}
import com.zongteng.ztetl.etl.stream.gc_wms_orders.fact.StreamGcWmsOrdersUtil.dayNowSub
import com.zongteng.ztetl.util.DataNullUtil.{nvl, nvlString}
import com.zongteng.ztetl.util.DateUtil
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object Sync_order_mongodb {

  private val gson: Gson = new Gson()

  def main(args: Array[String]): Unit = {

    //作业名称
    val appName = "Sync_order_mongodb"

    //消费时间间隔
    val interval = 5

    //kafka中topic名称
    val topic_name = "mysql_binlog"

    //消费组
    val group_Id = "consumer_mysql_binlog"

    val sparkStreamAndKafkaStream: (StreamingContext, InputDStream[ConsumerRecord[String, String]]) = SparkKafkaCon.getConnectKafka(appName, interval, topic_name, group_Id)
    val sparkStream = sparkStreamAndKafkaStream._1
    val kafkaDStream = sparkStreamAndKafkaStream._2


    try {
      //如果使用SparkStream和Kafka直连方式整合，生成的kafkaDStream必须调用foreachRDD
      kafkaDStream.foreachRDD(kafkaRDD => {

        if (!kafkaRDD.isEmpty()) {
          //获取当前批次的RDD的偏移量
          val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

          //拿出kafka中的数据
          val lines = kafkaRDD.map(_.value())

          //过滤出需要处理的数据
          lines.filter(f => f.contains("TmsTracking.sync_order")).foreach((x: String) => {
            //获取数据库连接池
            val connection: Connection = JDBCUtil.getConnection(JDBCUtil.jdbc_zt75_streaming)
            //关闭自动提交,使用手动提交
            connection.setAutoCommit(false)
            var delstatement: PreparedStatement = null
            var insstatement: PreparedStatement = null
            var updstatement: PreparedStatement = null
            var selstatement: PreparedStatement = null
            var resultSet: ResultSet = null

            try {
              val json = new JsonParser()
              val obj = json.parse(x).asInstanceOf[JsonObject]

              //mongodb数据库名和表名
              val datasource_name = "TmsTracking"
              val mongodb_table_name = "sync_order"
              val deleteSql = "DELETE FROM zongteng_streaming.TmsTracking_sync_order WHERE id = ?"
              val insertSql = "INSERT INTO zongteng_streaming.TmsTracking_sync_order " +
                " (id,TmsId,OrderId,TrackingNumber,WarehouseCode,TimezoneOriginatingPlace,UserId,ChannelName,DeliveryMethod,CreationTime,AScanDate,DScanDate,data_process_status,w_insert_dt)" +
                "  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
              val selectSql = "select * FROM zongteng_streaming.TmsTracking_sync_order WHERE id = ?"
              var data_process_status = ""
              var id = ""

              //再次判断
              if ("TmsTracking.sync_order".equalsIgnoreCase(nvl(obj.get("ns")))) {
                var op = nvl(obj.get("op"))
                if ("i".equalsIgnoreCase(op)) {
                  //获取值
                  val sync_order_insert: Sync_order_Insert = gson.fromJson(x, classOf[Sync_order_Insert])
                  data_process_status = "insert"
                  id = nvlString(sync_order_insert.o._id.$oid)
                  val TmsId = nvlString(sync_order_insert.o.TmsId)
                  val OrderId = nvlString(sync_order_insert.o.OrderId)
                  val TrackingNumber = nvlString(sync_order_insert.o.TrackingNumber)
                  val WarehouseCode = nvlString(sync_order_insert.o.WarehouseCode)
                  val TimezoneOriginatingPlace = nvlString(sync_order_insert.o.TimezoneOriginatingPlace)
                  val UserId = nvlString(sync_order_insert.o.UserId)
                  val ChannelName = nvlString(sync_order_insert.o.ChannelName)
                  val DeliveryMethod = nvlString(sync_order_insert.o.DeliveryMethod)
                  val CreationTime = nvlDate(sync_order_insert.o.CreationTime)
                  val AScanDate = nvlDate(sync_order_insert.o.AScanDate)
                  val DScanDate = nvlDate(sync_order_insert.o.DScanDate)
                  val w_insert_dt: String = DateUtil.getNow()

                  println(s"==============${datasource_name + "_" + mongodb_table_name}=====进入=================")

                  //预编译
                  delstatement = connection.prepareCall(deleteSql)
                  insstatement = connection.prepareCall(insertSql)

                  //删除
                  delstatement.setString(1, id)

                  //新增
                  insstatement.setString(1, id)
                  insstatement.setInt(2, if (TmsId == null || "".equalsIgnoreCase(TmsId)) -1 else TmsId.toInt)
                  insstatement.setString(3, OrderId)
                  insstatement.setString(4, TrackingNumber)
                  insstatement.setString(5, WarehouseCode)
                  insstatement.setInt(6, if (TimezoneOriginatingPlace == null || "".equalsIgnoreCase(TimezoneOriginatingPlace)) 0 else TimezoneOriginatingPlace.toInt)
                  insstatement.setInt(7, if (UserId == null || "".equalsIgnoreCase(UserId)) 0 else UserId.toInt)
                  insstatement.setString(8, ChannelName)
                  insstatement.setString(9, DeliveryMethod)
                  insstatement.setString(10, StreamTypeParaUtil.nvlDateTime(CreationTime))
                  insstatement.setString(11, StreamTypeParaUtil.nvlDateTime(AScanDate))
                  insstatement.setString(12, StreamTypeParaUtil.nvlDateTime(DScanDate))
                  insstatement.setString(13, data_process_status)
                  insstatement.setString(14, w_insert_dt)

                  //判断相差的时间天数,只取五天之内的数据
                  val day: Int = dayNowSub(CreationTime)
                  //执行语句
                  delstatement.executeUpdate()
                  insstatement.executeUpdate()
                  //提交
                  connection.commit()
                  println(s"${datasource_name + "_" + mongodb_table_name} == " + "sync_order")
                  println(s"${datasource_name + "_" + mongodb_table_name}=====提交成功=================")

                  /*if (Array("9", "11").contains(UserId) && day < 5) {
                    //执行语句
                    delstatement.executeUpdate()
                    insstatement.executeUpdate()
                    //提交
                    connection.commit()
                    println(s"${datasource_name + "_" + mongodb_table_name} == " + "sync_order")
                    println(s"${datasource_name + "_" + mongodb_table_name}=====提交成功=================")
                  }*/

                } else if ("u".equalsIgnoreCase(op)) {
                  //获取数据
                  var sync_order_update: Sync_order_update = gson.fromJson(x, classOf[Sync_order_update])
                  data_process_status = "update"

                  id = nvlString(sync_order_update.id)
                  //判断修改的语句是否在mysql中存在(不存在则不执行)
                  //预编译
                  selstatement = connection.prepareCall(selectSql)
                  selstatement.setString(1, id)

                  //执行
                  resultSet = selstatement.executeQuery()
                  //判断返回值是否存在
                  if (resultSet.next()) {
                    // 1.确定那些字段修改
                    val strArray: Array[String] = Array("id", "TmsId", "OrderId", "TrackingNumber", "WarehouseCode", "TimezoneOriginatingPlace", "UserId", "ChannelName", "DeliveryMethod", "CreationTime", "AScanDate", "DScanDate")

                    val fields: Array[Field] = Class.forName("com.zongteng.ztetl.entity.common.Sync_order_title").getDeclaredFields.filter((f: Field) => {
                      x.contains(f.getName())
                    }).filter((f: Field) => {
                      strArray.contains(f.getName())
                    })

                    // 2.将样例类转成转成map并赋值
                    val map: util.HashMap[String, Object] = gson.fromJson(gson.toJson(sync_order_update.o.$set), classOf[util.HashMap[String, Object]])

                    // 3.确定主键
                    //调用语句的时候已经得到了,可以直接调度

                    // 3.拼接UPATE的sql
                    val sql = s"UPDATE zongteng_streaming.TmsTracking_sync_order SET updateSql WHERE id = '$id'"

                    val str: String = fields.map((f: Field) => {
                      val name: String = f.getName
                      var value = map.get(name)

                      if (value != null && !"".equals(value.toString.trim) && value.toString.contains("$date")) {
                        val str: String = value.toString
                        val start: Int = str.indexOf("=")
                        val end: Int = str.lastIndexOf("}")

                        val value3: String = DateUtil.getBeijingTimes(str.substring(start + 1, end))
                        s"$name = '$value3'"
                      } else {
                        s"$name = '$value'"
                      }
                    }).mkString(",")

                    var updateSql: String = ""
                    if (StringUtils.isNotBlank(str)) {
                      updateSql = sql.replace("updateSql", str.concat(s",data_process_status='$data_process_status',w_update_dt='${DateUtil.getNow()}'"))
                    } else {
                      updateSql = sql.replace("updateSql", s"data_process_status='$data_process_status',w_update_dt='${DateUtil.getNow()}'")
                    }


                    //println(updateSql)
                    // 4.预编译
                    updstatement = connection.prepareCall(updateSql)

                    // 5.执行
                    val i: Int = updstatement.executeUpdate()
                    connection.commit()

                    println(s"修改${i}条数据")
                  }
                  println(s"mysql中不存在id为${id}的数据")
                } else if ("d".equalsIgnoreCase(op)) {
                  //预编译
                  delstatement = connection.prepareCall(deleteSql)

                  //获取主键
                  id = nvl(obj.get("id"))
                  delstatement.setString(1, id)
                  //执行语句
                  delstatement.executeUpdate()
                  //提交
                  connection.commit()
                  println(s"删除id为${id}语句成功")
                }

              }

            } catch {
              case e: Exception => {
                if (connection != null) connection.rollback() // 回滚
                throw e
              }
            } finally {
              JDBCUtil.close(resultSet, delstatement, connection)
              JDBCUtil.close(resultSet, insstatement, connection)
            }

          })
          //提交对应kafka的偏移量
          kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

        }
      })
    } catch {
      case e: Exception => e.getMessage
        kafkaDStream.stop()
    }

    sparkStream.start()
    sparkStream.awaitTermination()
  }

  def nvlDate(str: Sync_order_date): String = {
    if (str != null) DateUtil.getBeijingTimes(str.$date) else ""
  }

}
