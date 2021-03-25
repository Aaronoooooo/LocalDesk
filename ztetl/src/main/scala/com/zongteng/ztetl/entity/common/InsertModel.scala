package com.zongteng.ztetl.entity.common

import java.util

case class InsertModel(
                        data: Array[util.HashMap[String, String]],
                        database: String,
                        es: String,
                        id: String,
                        isDdl: String,
                        mysqlType: util.HashMap[String, String],
                        old: Array[util.HashMap[String, String]],
                        pkNames: Array[String],
                        sql: String,
                        sqlType: util.HashMap[String, String],
                        table: String,
                        ts: String,
                        `type`: String
                      )
