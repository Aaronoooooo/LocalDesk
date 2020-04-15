package com.zongteng.ztetl.etl.stream.gc_wms_orders.fact

import java.io
import java.sql.{Connection, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Date

import com.zongteng.ztetl.api.SparkKafkaCon
import com.zongteng.ztetl.common.jdbc.JDBCUtil
import com.zongteng.ztetl.common.kafka.offset.KafkaOffsetManager
import com.zongteng.ztetl.entity.common.InsertModel
import com.zongteng.ztetl.entity.gc_wms.{OrderOperationTime, OrderPhysicalRelation, Orders}
import com.zongteng.ztetl.etl.stream.Stream_json_util
import com.zongteng.ztetl.util.DateUtil
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object StreamGcWmsOrdersUtil {

  /**
    * 生成实时导入ods层代码的工具类
    *
    * @param appName        作业名称
    * @param interval       消费时间间隔
    * @param topics     kafka中topic名称
    * @param groupId       消费组
    * @param mysql_database mysql数据库名称
    * @param datasource_name 数据来源简称（例如：gc_oms）
    */
  def getRunCode(appName: String, interval: Int, topics: Array[String], groupId: String, mysql_database: String, datasource_name: String) = {

    val skzzz  = SparkKafkaCon.getSparkStream$KafkaStrem$ZkClient$ZkUtilsByOffset(appName, interval, groupId, topics, true)
    val sparkStream = skzzz._1
    val kafkaStream = skzzz._2
    val zkClient = skzzz._3
    val zkUtils = skzzz._4
    val zkOffsetPath = skzzz._5

    try {

      kafkaStream.foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) => {

        println("isEmpty == "  + rdd.isEmpty())

        if (!rdd.isEmpty()) {

          println("数据量 == " + rdd.count())

           // filter作用：只消费指定数据库指定表的数据
          rdd.filter((e: ConsumerRecord[String, String]) => checkJsonStr(mysql_database, e.value(), datasource_name)).foreachPartition((cols: Iterator[ConsumerRecord[String, String]]) => {

            val connection: Connection = JDBCUtil.getConnection(JDBCUtil.jdbc_zt75_streaming)

            connection.setAutoCommit(false)
            var delstatement: PreparedStatement = null
            var delstatement2: PreparedStatement = null

            var insstatement: PreparedStatement = null
            try {
              cols.foreach((e: ConsumerRecord[String, String]) => {
                println(e.value())
                // 获取对应的row数据
                val model: InsertModel = Stream_json_util.getInsertObject(e.value())
                val mysql_table_name = model.table
                val opr_type: String = model.`type`

                if ("orders".equals(mysql_table_name)) { // 订单表
                  println(s"==============${datasource_name + "_" + mysql_table_name}=====进入=================")
                  val clazz = classOf[Orders]
                  val serializables: ListBuffer[io.Serializable] = Stream_json_util.getCaseClass(model, clazz)

                  val deleteSql = "DELETE FROM gc_wms_orders WHERE order_id = ?"
                  val deleteSql2 = "DELETE FROM gc_wms_orders WHERE order_code = ?"
                  val insertSql = "INSERT INTO gc_wms_orders VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                                                                        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                                                                        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                  delstatement = connection.prepareCall(deleteSql)
                  delstatement2 = connection.prepareCall(deleteSql2)
                  insstatement = connection.prepareCall(insertSql)

                  for (i <- 0 until serializables.length) {
                    val caseClass: Orders = serializables(i).asInstanceOf[Orders]

                    // 删除
                    delstatement.setInt(1, caseClass.order_id.toInt)

                    delstatement2.setString(1, caseClass.order_code)

                    // 新增
                    insstatement.setInt(1, caseClass.order_id.toInt)
                    insstatement.setString(2, if (caseClass.order_code == null) "" else caseClass.order_code)
                    insstatement.setString(3, caseClass.reference_no)
                    insstatement.setString(4, if (caseClass.customer_code == null) "" else caseClass.customer_code)
                    insstatement.setString(5, if (caseClass.platform == null) "" else caseClass.platform)
                    insstatement.setString(6, if (caseClass.create_type == null) "" else caseClass.create_type)
                    insstatement.setInt(7, if (caseClass.warehouse_id == null) 0 else caseClass.warehouse_id.toInt)
                    insstatement.setInt(8, if (caseClass.is_oda == null) 0 else caseClass.is_oda.toInt)
                    insstatement.setInt(9, if (caseClass.is_insurance == null) 0 else caseClass.is_insurance.toInt)
                    insstatement.setString(10, caseClass.sm_code)
                    insstatement.setInt(11, if (caseClass.parcel_quantity == null) 0 else caseClass.parcel_quantity.toInt)
                    insstatement.setInt(12, if (caseClass.order_status == null) 1 else caseClass.order_status.toInt)
                    insstatement.setInt(13, if (caseClass.problem_status == null) 0 else caseClass.problem_status.toInt)
                    insstatement.setInt(14, if (caseClass.underreview_status == null) 0 else caseClass.underreview_status.toInt)
                    insstatement.setInt(15, if (caseClass.upload_express_status == null) 0 else caseClass.upload_express_status.toInt)
                    insstatement.setInt(16, if (caseClass.anew_express_status == null) 0 else caseClass.anew_express_status.toInt)
                    insstatement.setInt(17, if (caseClass.intercept_status == null) 0 else caseClass.intercept_status.toInt)
                    insstatement.setInt(18, if (caseClass.sync_status == null) 0 else caseClass.sync_status.toInt)
                    insstatement.setString(19, StreamTypeParaUtil.nvlDateTime(caseClass.add_time))
                    insstatement.setInt(20, if (caseClass.order_pick_type == null) 0 else caseClass.order_pick_type.toInt)
                    insstatement.setInt(21, if (caseClass.sc_id == null) 0 else caseClass.sc_id.toInt)
                    insstatement.setString(22, StreamTypeParaUtil.nvlDateTime(caseClass.sync_wms_time))
                    insstatement.setString(23, if (caseClass.operator_note == null) "" else caseClass.operator_note)
                    insstatement.setInt(24, if (caseClass.is_fba == null) 0 else caseClass.is_fba.toInt)
                    insstatement.setString(25, StreamTypeParaUtil.nvlDateTime(caseClass.outbound_time))
                    insstatement.setInt(26, if (caseClass.is_more_box == null) 0 else caseClass.is_more_box.toInt)
                    insstatement.setString(27, StreamTypeParaUtil.nvlDateTime(caseClass.o_timestamp))
                    insstatement.setString(28, StreamTypeParaUtil.nvlDateTime(caseClass.payment_time))
                    insstatement.setString(29, StreamTypeParaUtil.nvlDateTime(caseClass.oms_date_create))
                    insstatement.setString(30, StreamTypeParaUtil.nvlDateTime(caseClass.pre_delivery_time))
                    insstatement.setInt(31, if (caseClass.is_flow_volume == null) 1 else caseClass.is_flow_volume.toInt)
                    insstatement.setString(32, DateUtil.getNow())
                    insstatement.setString(33, model.`type`)

                    val customer_code: String = caseClass.customer_code
                    val order_type: String = caseClass.order_type

                    val day: Int = dayNowSub(caseClass.add_time)

                    if (!Array("000010", "000016", "245").contains(customer_code) && "0".equals(order_type) && day <= 5) {
                     if (executeSql(opr_type, insstatement, delstatement, delstatement2)) {
                       connection.commit()
                       println(s"${datasource_name + "_" + mysql_table_name} == " + caseClass)
                       println(s"${datasource_name + "_" + mysql_table_name}=====提交成功=================")
                     }
                    }

                  }
                }
                else if ("order_operation_time".equals(mysql_table_name)) { // 订单操作时间

                  println(s"==============${datasource_name + "_" + mysql_table_name}=====进入=================")

                  val clazz = classOf[OrderOperationTime]
                  val serializables: ListBuffer[io.Serializable] = Stream_json_util.getCaseClass(model, clazz)

                  val deleteSql = "DELETE FROM gc_wms_order_operation_time WHERE oot_id = ?"
                  val deleteSql2 = "DELETE FROM gc_wms_order_operation_time WHERE order_id = ?"
                  val insertSql = "INSERT INTO gc_wms_order_operation_time VALUES(?, ?, ?, ?, ?, ?, ?, ?)"

                  delstatement = connection.prepareCall(deleteSql)
                  delstatement2 = connection.prepareCall(deleteSql2)
                  insstatement = connection.prepareCall(insertSql)

                  for (i <- 0 until serializables.length) {
                    val caseClass: OrderOperationTime = serializables(i).asInstanceOf[OrderOperationTime]

                    delstatement.setInt(1, caseClass.oot_id.toInt)

                    delstatement2.setInt(1, caseClass.order_id.toInt)

                    insstatement.setInt(1, caseClass.oot_id.toInt)
                    insstatement.setInt(2, if (caseClass.order_id == null) 0 else caseClass.order_id.toInt)
                    insstatement.setString(3, StreamTypeParaUtil.nvlDateTime(caseClass.process_time))
                    insstatement.setString(4, StreamTypeParaUtil.nvlDateTime(caseClass.pack_time))
                    insstatement.setString(5, StreamTypeParaUtil.nvlDateTime(caseClass.ship_time))
                    insstatement.setString(6, StreamTypeParaUtil.nvlDateTime(caseClass.import_time))
                    insstatement.setString(7, DateUtil.getNow())
                    insstatement.setString(8, model.`type`)

                    if (executeSql(opr_type, insstatement, delstatement, delstatement2)) {
                      connection.commit()
                      println(s"${datasource_name + "_" + mysql_table_name} == " + caseClass)
                      println(s"${datasource_name + "_" + mysql_table_name}=====提交成功=================")
                    }
                  }

                }
                else if ("order_physical_relation".equals(mysql_table_name)) { // 订单物理仓
                  println(s"==============${datasource_name + "_" + mysql_table_name}=====进入=================")
                  val clazz: Class[OrderPhysicalRelation] = classOf[OrderPhysicalRelation]
                  val serializables: ListBuffer[io.Serializable] = Stream_json_util.getCaseClass(model, clazz)

                  val deleteSql = "DELETE FROM gc_wms_order_physical_relation WHERE opr_id = ?"
                  val deleteSql2 = "DELETE FROM gc_wms_order_physical_relation WHERE wp_code = ? AND order_code = ? "
                  val insertSql = "INSERT INTO gc_wms_order_physical_relation VALUES(?, ?, ?, ?, ?)"

                  delstatement = connection.prepareCall(deleteSql)
                  delstatement2 = connection.prepareCall(deleteSql2)
                  insstatement = connection.prepareCall(insertSql)

                  for (i <- 0 until serializables.length) {

                    val caseClass: OrderPhysicalRelation = serializables(i).asInstanceOf[OrderPhysicalRelation]

                    delstatement.setInt(1, caseClass.opr_id.toInt)

                    delstatement2.setString(1, caseClass.wp_code)
                    delstatement2.setString(2, caseClass.order_code)

                    insstatement.setInt(1, caseClass.opr_id.toInt)
                    insstatement.setString(2, caseClass.wp_code)
                    insstatement.setString(3, caseClass.order_code)
                    insstatement.setString(4, DateUtil.getNow())
                    insstatement.setString(5, model.`type`)

                    if (executeSql(opr_type, insstatement, delstatement, delstatement2)) {
                      connection.commit()
                      println(s"${datasource_name + "_" + mysql_table_name} == " + caseClass)
                      println(s"${datasource_name + "_" + mysql_table_name}=====提交成功=================")
                    }
                  }

                }
              })

            } catch {
              case e: Exception => {
                if (connection != null) connection.rollback() // 回滚
                throw e
              }
            } finally {
              JDBCUtil.close(null, delstatement, connection)
              JDBCUtil.close(null, insstatement, connection)
            }

          })

          KafkaOffsetManager.savaOffsets(zkUtils, zkClient, zkOffsetPath, rdd)
        }
      })

      //Log.end(task)
    } catch {
      case e: Exception => e.getMessage
        sparkStream.stop()
        //Log.end(task)
    }

    sparkStream.start()
    sparkStream.awaitTermination()
  }

  /**
    * add_time和现在相差几天
    * @param add_time
    * @return
    */
  def dayNowSub(add_time: String) = {

    var day: Int = 1000

    if (StreamTypeParaUtil.nvlDateTime(add_time) != null && add_time.length >= 10 && add_time.split("-").length == 3) {
      val format = new SimpleDateFormat("yyyy-MM-dd")

      val now: String = DateUtil.format(new Date(), "yyyy-MM-dd")
      val now_time = format.parse(now).getTime
      val add_time2 = format.parse(add_time.substring(0, 10)).getTime

      day = ((now_time - add_time2) / 1000 / 60 / 60 / 24).toInt
    }

    day
  }

  /**
    * 执行sql，如果执行成功返回true，执行失败返回false
    * @param opr_type
    * @param insstatement
    * @param delstatements
    * @return
    */
  def executeSql(opr_type: String, insstatement: PreparedStatement,  delstatements: PreparedStatement*) = {

    var result: Boolean = false

    if ("UPDATE".equalsIgnoreCase(opr_type)) {
      delstatements.foreach(_.executeUpdate())
      insstatement.executeUpdate()
      result = true
    } else if ("DELETE".equalsIgnoreCase(opr_type)) {
      delstatements.foreach(_.executeUpdate())
      result = true
    } else if ("INSERT".equalsIgnoreCase(opr_type)) {
      delstatements.foreach(_.executeUpdate())
      insstatement.executeUpdate()
      result = true
    }

    result
  }


  def checkJsonStr(mysql_database: String, jsonStr: String, datasource_name: String) = {

    val allow_mysql_table: Array[String] = Orders_stream_allow_mysql_table.table
    var result = false

    if (StringUtils.isNotBlank(jsonStr)) {
      val model: InsertModel = Stream_json_util.getInsertObject(jsonStr)
      result = mysql_database.equalsIgnoreCase(model.database) &&
        allow_mysql_table.contains(datasource_name + "_" + model.table)
    }

    result
  }

}
