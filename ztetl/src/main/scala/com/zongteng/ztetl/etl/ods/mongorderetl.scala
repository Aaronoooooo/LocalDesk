package com.zongteng.ztetl.etl.ods

import java.util

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import com.zongteng.ztetl.api.SparkConfig
import com.zongteng.ztetl.common.ParValListToHbase.insertToHbase
import com.zongteng.ztetl.util.DataNullUtil.nvlObject
import com.zongteng.ztetl.util.{DateUtil, PropertyFile}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * MongoDB to Hive
  * Mingwen,pan
  */
object mongorderetl {

  // 最终得到的put集合
  private var putList: util.ArrayList[Put] = null
  // hbase列族
  private val column_family: String = "order"
  // dw层hbase表名
  private val table_name: String = "TmsTracking_sync_order_full"

  private var put: Put = null


  def main(args: Array[String]): Unit = {

    val master = PropertyFile.getProperty("master")

    val spark: SparkSession = SparkConfig.getSqlContext("mongorderetl")
    val sc = new SparkConf().setAppName("sync_order").setMaster(master).set("spark.mongodb.input.uri", "mongodb://datateam:efHMfsHY9YrBz_my@36.248.12.186:63101/TmsTracking.sync_order?authSource=TmsTracking")
    val readConfig = ReadConfig(Map("collection" -> "sync_order", "batchSize" -> "100"), Some(ReadConfig(sc)))

    val mongoRdd: MongoRDD[java.util.HashMap[String, Object]] = MongoSpark.load[java.util.HashMap[String, Object]](spark.sparkContext, readConfig)

    //获取样例类中的每个字段名称
    val fields: Array[String] = Class.forName("com.zongteng.ztetl.entity.common.SysOrder").getDeclaredFields.map(_.getName)

    putList = new java.util.ArrayList[Put]

    //需要处理的时间字段
    val timeArr: Array[String] = Array[String]("CreationTime", "UpdateTime", "SyncTime", "LastTrackingTime", "TrackingChangeTime", "NextTrackingTime", "AScanDate", "DScanDate")

    var num: Int = 0

    mongoRdd.foreach(x => {
      //将数据写入hbase中,
      if (putList != null && putList.size() >= 10) {
        num += insertToHbase(table_name, putList)
        println("一共插入了" + num + "条数据")
        putList.clone()
        putList = new java.util.ArrayList[Put]
      }

      //得到rowkey
      fields.foreach(f => {
        if ("_id".equalsIgnoreCase(f)) {
          put = new Put(Bytes.toBytes(nvlObject(x.get(f))))
          //println(nvlObject(x.get(f)))
        }
      })

      //获取其它值
      fields.foreach(field => {
        if (x.get(field) != null && !"_id".equalsIgnoreCase(field) && !"".equalsIgnoreCase(nvlObject(x.get(field)))) {
          if (timeArr.contains(field)) {
            put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(field), Bytes.toBytes(DateUtil.getEnglishTimes(nvlObject(x.get(field)))))
            //if("CreationTime".equalsIgnoreCase(field) && "00".equalsIgnoreCase(DateUtil.getEnglishTimes(nvlObject(x.get(field))).substring(11,13)))  println(nvlObject(x.get(field)))

          } else {
            put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(field), Bytes.toBytes(nvlObject(x.get(field))))
          }
        }
      })

      put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("data_process_status"), Bytes.toBytes("batch"))
      put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_insert_dt"), Bytes.toBytes(DateUtil.getNow()))
      put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_update_dt"), Bytes.toBytes(DateUtil.getNow()))

      putList.add(put)
    })

    //再调用一次将数据写入hbase中,防止最后一次不满次数
    num += insertToHbase(table_name, putList)

    println("一共插入了" + num + "条数据")

    sc.clone()
    spark.close()


  }

}