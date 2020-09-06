package com.flinkkylin

import org.apache.flink.api.scala._
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import java.util.Date

/**
  * @author pengfeisu
  * @create 2020-02-11 13:32
  */
object FlinkToKylin {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read data from JDBC (Kylin in our case)
    var longColum: TypeInformation[Long] = createTypeInformation[Long]
    var bigDecimalColum: TypeInformation[BigDecimal] = createTypeInformation[BigDecimal]
    var dateColum: TypeInformation[Date] = createTypeInformation[Date]
//    var stringColum: TypeInformation[String] = createTypeInformation[String]

    val DB_ROWTYPE = new RowTypeInfo(dateColum,bigDecimalColum,longColum)
    //val DB_ROWTYPE = new RowTypeInfo(longColum)
    //val DB_ROWTYPE = new RowTypeInfo(Seq(longColum))
    val inputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("org.apache.kylin.jdbc.Driver")
      .setDBUrl("jdbc:kylin://cdh101:52370/Fly_Model_Test101")
      .setUsername("ADMIN")
      .setPassword("KYLIN")
      .setQuery("select part_dt, sum(price) as total_selled, count(distinct TRANS_ID) as sellers from kylin_sales group by part_dt order by part_dt")
      //.setQuery("select count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt")
      .setRowTypeInfo(DB_ROWTYPE)
      .finish()
    val dataset =env.createInput(inputFormat)
    dataset.print()

  }
}
