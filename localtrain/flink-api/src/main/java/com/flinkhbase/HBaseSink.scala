package com.flinkhbase

import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}

class HBaseSink(tableName: String, family: String) extends RichSinkFunction[String] {


  var conn: Connection = _
  var info1_new: String = "info1_new"
  var info2_new: String = "info2_new"
  var table_new: String = "student_new"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1")
    conf.set("hbase.zookeeper.quorum", "cdh101")
    conf.set("hbase.zookeeper.property.clientPort", "32181")
    /*regionServer_Master外网映射端口必须与默认端口16020一致(关闭regionServer_slave)
    conf.set("hbase.regionserver.port", "16020")
    HMaster外网映射端口必须与默认端口16000一致
    conf.set("hbase.master.port", "16000")*/
    conn = ConnectionFactory.createConnection(conf)
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val g = new Gson()
    val student = g.fromJson(value, classOf[Student])
    println(value + "_value")
    println(student)

    val t: Table = conn.getTable(TableName.valueOf(table_new))
    println("tableName:" + t.getName)

    val admin = conn.getAdmin
    if (admin.tableExists(t.getName)) {
      println("表存在")
    } else {
      //创建表对象
      //      val hd = new HTableDescriptor(TableName.valueOf(tableName))
      val hd = new HTableDescriptor(TableName.valueOf(table_new))
      //创建列族对象
      val hc1 = new HColumnDescriptor(info1_new.getBytes)
      val hc2 = new HColumnDescriptor(info2_new.getBytes)
      //设置列族保存最大历史版本
      hc1.setMaxVersions(1)
      hc2.setMaxVersions(1)
      hd.addFamily(hc1)
      hd.addFamily(hc2)
      //创建表
      admin.createTable(hd)
      //关闭连接
      admin.close()
    }

    //创建一个Put对象来执行更新操作，创建对象时需要给定一个行名ROW
    val put: Put = new Put(Bytes.toBytes(student.sid))
    //    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(student.name))
    //    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(student.age))
    //    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"), Bytes.toBytes(student.sex))
    put.addColumn(Bytes.toBytes(info1_new), Bytes.toBytes("sex"), Bytes.toBytes(student.sex))
    put.addColumn(Bytes.toBytes(info2_new), Bytes.toBytes("age"), Bytes.toBytes(student.age))
    t.put(put)
    t.close()
  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }
}
