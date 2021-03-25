package com.zongteng.ztetl.entity.common

case class MysqlOdsScript (
                          mysqlDataSource: String, // ZY_WMS, GC_WMS, GC_OMS
                          mysqlTable: String,
                          primaryKey: String,
                          createTime: String,
                          updataTime_1: String,
                          updataTime_2: String,
                          columnFamily: String,
                          tableType: String,
                          connect: String,
                          username: String,
                          password: String,
                          schema: String,
                          datasource_num_id: String,
                          mysql_datasource_name: String
                          )
