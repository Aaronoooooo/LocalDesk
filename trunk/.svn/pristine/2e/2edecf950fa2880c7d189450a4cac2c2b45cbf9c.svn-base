package com.zongteng.ztetl.etl.stream.gc_lms

import java.sql.{Connection, Statement}

import com.zongteng.ztetl.common.jdbc.JDBCUtil

object ContainerDetailsCreateTable {

  val sql = "DROP TABLE IF EXISTS stream_gc_lms_container_details;\n" +
    "CREATE TABLE `stream_gc_lms_container_details` (\n" +
    "  row_wid bigint NOT NULL COMMENT 'datasource_num_id + container_details_id',\n" +
    "  datasource_num_id varchar(50) NOT NULL COMMENT '数据的owms那个数据库（9032、9033、9034等等）',\n" +
    "  `container_details_id` int(11) NOT NULL COMMENT '容器详情id',\n" +
    "  `container_id` int(11) NOT NULL COMMENT '容器id',\n" +
    "  `container_code` varchar(50) NOT NULL COMMENT '容器号',\n" +
    "  `order_number` varchar(50) NOT NULL COMMENT '订单号',\n" +
    "  `tracking_number` varchar(50) NOT NULL COMMENT '跟踪号',\n" +
    "  `channel_code` varchar(50) NOT NULL COMMENT '渠道代码',\n" +
    "  `loader_time` datetime DEFAULT NULL COMMENT '装车时间',\n" +
    "  `shipper_time` datetime DEFAULT NULL COMMENT '交运时间',\n" +
    "  w_insert_dt timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据插入的时间戳',\n" +
    "  data_flag varchar(32) NOT NULL,\n" +
    "  PRIMARY KEY (`row_wid`),\n" +
    "  KEY `container_id` (`container_id`) USING BTREE\n" +
    ") ENGINE=InnoDB AUTO_INCREMENT=104686 DEFAULT CHARSET=utf8 COMMENT='容器详情';\n" +
    "DROP TABLE IF EXISTS staging_stream_gc_lms_container_details;\n" +
    "CREATE TABLE `staging_stream_gc_lms_container_details` (\n" +
    "  row_wid bigint NOT NULL COMMENT 'datasource_num_id + container_details_id',\n" +
    "  datasource_num_id varchar(50) NOT NULL COMMENT '数据的owms那个数据库（9032、9033、9034等等）',\n" +
    "  `container_details_id` int(11) NOT NULL COMMENT '容器详情id',\n" +
    "  `container_id` int(11) NOT NULL COMMENT '容器id',\n" +
    "  `container_code` varchar(50) NOT NULL COMMENT '容器号',\n" +
    "  `order_number` varchar(50) NOT NULL COMMENT '订单号',\n" +
    "  `tracking_number` varchar(50) NOT NULL COMMENT '跟踪号',\n" +
    "  `channel_code` varchar(50) NOT NULL COMMENT '渠道代码',\n" +
    "  `loader_time` datetime DEFAULT NULL COMMENT '装车时间',\n" +
    "  `shipper_time` datetime DEFAULT NULL COMMENT '交运时间',\n" +
    "  w_insert_dt timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据插入的时间戳',\n" +
    "  data_flag varchar(32) NOT NULL,\n" +
    "  PRIMARY KEY (`row_wid`),\n" +
    "  KEY `container_id` (`container_id`) USING BTREE\n" +
    ") ENGINE=InnoDB AUTO_INCREMENT=104686 DEFAULT CHARSET=utf8 COMMENT='容器详情';"

  def main(args: Array[String]): Unit = {

    var connection: Connection = null

    var createStatement: Statement = null

    try {
      connection = JDBCUtil.getConnection(JDBCUtil.jdbc_zt75_streaming)
      connection.setAutoCommit(false)

      // 预编译
      createStatement = connection.createStatement()

      sql.split(";").foreach((sql: String) => {
        createStatement.addBatch(sql + ";")
      })

     val num: Array[Int] = createStatement.executeBatch()

      if(num.size == 4) {
          println("stream_gc_lms_container_details 建表成功")
          println("staging_stream_gc_lms_container_details 建表成功")
      }

      // 提交事务
      connection.commit()
    } catch {
      case e: Exception => {
        if (connection != null) connection.rollback() // 回滚
        throw e
      }
    } finally {
      JDBCUtil.close(null, createStatement, connection)
    }

  }







}
