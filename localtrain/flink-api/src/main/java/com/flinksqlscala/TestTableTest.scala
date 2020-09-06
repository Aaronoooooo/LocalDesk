package com.flinksqlscala

import java.util
import java.util.{ArrayList, List, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.http.HttpHost

case class Students(className: String, subject: String, college: String)

object TestTableTest {
  def main(args: Array[String]): Unit = {
    TestFlinkSql()
  }

  def TestFlinkSql(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEenv = TableEnvironment.getTableEnvironment(env)

    //连接kafka
    val properties = new Properties()
    //properties.setProperty("bootstrap.servers", "cdh101:54092")
    properties.setProperty("bootstrap.servers", "flydiysz.cn:31092")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "sg9z971")
    properties.setProperty("auto.offset.reset", "earliest");

    val httpHosts: util.List[HttpHost] = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("flydiysz.cn", 52200, "http"))
    httpHosts.add(new HttpHost("cdh102", 52200, "http"))
    httpHosts.add(new HttpHost("cdh103", 52200, "http"))

    val inputStream1: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
      "flink_table", new SimpleStringSchema(), properties))
    inputStream1.print("flink_table").setParallelism(1)
    val transDataStream1 = inputStream1.map(data => {
      val dataArray = data.split(",")
      Students(dataArray(0), dataArray(1), dataArray(2))
    })
    println("**************************************")

    val inputStream2: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
      "flink_sql", new SimpleStringSchema(), properties))
    inputStream2.print("flink_sql").setParallelism(1)

    val transDataStream2 = inputStream2.map(data => {
      val dataArray = data.split(",")
      Students(dataArray(0), dataArray(1), dataArray(2))
    })
    println("######################################")

    /**
      * 将DataStream注册为Table,注册成catalog中的表,指定表名和字段,且表名唯一,字段类型自动推断;
      * 重命名默认字段className,subject,college为
      * ’classNames,’subjects,’colleges
      */
    val studentTable1 = tEenv.registerDataStream("StudentTable", transDataStream1, 'classNames, 'subjects, 'colleges)
    val studentTable3 = tEenv.registerDataStream("StudentTable3", transDataStream2, 'name, 'role, 'company)

    /**
      * 将DataStream转换为Table,指定字段,字段类型自动推断;
      * 重命名默认字段className,subject,college为
      * 'classNames,'subjects,'colleges
      */
    //    val studentTable4: Table = tEenv.fromDataStream(transDataStream2, '_1 as 'classNamessS, '_2 as 'subjectssS, '_3 as 'collegessS)
    val studentTable2 = transDataStream1.toTable(tEenv, 'classNamesS, 'subjectsS, 'collegesS)

    /**
      * 将Table转换为DataStream,在Table API/SQL环境中同时使用DataStream API处理数据;
      * RetractStream(第一个字段为True对应INSERT,False对应DELETE)
      */
    //    val st2DataStream = tEenv.toAppendStream(studentTable2)
    //    val st2RetractStream: DataStream[(Boolean, (String, String, String))] = tEenv.toRetractStream[(String, String, String)](studentTable2)

    //sql查询
    val st1: Table = tEenv.sqlQuery("select * from StudentTable")
    st1.printSchema()
    val t1Result = tEenv.toAppendStream[(String, String, String)](st1)
    t1Result.print("st1");

    val st2: Table = tEenv.sqlQuery(s"select * from $studentTable2")
    st2.printSchema()
    val t2Result = tEenv.toAppendStream[(String, String, String)](st2)
    t2Result.print("st2");

    //widentable
    val st3: Table = tEenv.sqlQuery("select * from StudentTable t1 join StudentTable3 t3 on t1.colleges=t3.company")
    st3.printSchema()
    val t3Result = tEenv.toAppendStream[(String, String, String, String, String, String)](st3)
    t3Result.print("st3");

    /*val tableResult1 = tEenv.sqlQuery(
      "select * from" + studentTable2 + "where classNames = apache join StudentTable on StudentTable.classNames = " + studentTable2 + ".classNames")
    tableResult1.printSchema()
    tableResult1.toAppendStream[(String, String, String, String)]
      .print("Append关联查询结果")*/

    /*val tableResult2 = tEenv.sqlQuery(
      "select * from" + studentTable2 + "where classNames = apache join StudentTable on StudentTable.classNames = " + studentTable2 + ".classNames")
    tableResult2.printSchema()
    tableResult2.toRetractStream[(String, String, String, String)]
      .print("Retract关联查询结果")*/
    env.execute()
  }
}