package com.mobikok.bigdata.spark.streaming.req.service

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat

import com.mobikok.bigdata.spark.streaming.req.bean.Ad_Click_Log
import com.mobikok.bigdata.spark.streaming.req.dao.DateAreaCityAdCountAnalysisDaoTest
import com.mobikok.dev.framework.core.TService
import com.mobikok.dev.framework.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
  * @author Aaron
  * @date 2020/07/29
  */
class DateAreaCityAdCountAnalysisServiceTest extends TService{
private val dateAreaCityAdCountAnalysisDaoTest = new DateAreaCityAdCountAnalysisDaoTest

  /**
    * 数据分析
    *
    * @return
    */
  override def analysis()= {
    val messageDS: DStream[String] = dateAreaCityAdCountAnalysisDaoTest.readKafka()
    val logDS: DStream[Ad_Click_Log] = messageDS.map(
      data => {
        val datas: Array[String] = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //数据结构转换按天聚合统计
    val sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm")
    val mapDS: DStream[((String, String, String, String), Int)] = logDS.map(
      log => {
        val ts: String = log.ts
        val day: String = sdf.format(new java.util.Date(ts.toLong))
        ((day, log.area, log.city, log.adid), 1)
      }
    )
    val reduceDS: DStream[((String, String, String, String), Int)] = mapDS.reduceByKey(_+_)
    reduceDS.foreachRDD(
      rdd => rdd.foreach(println)
    )

    //数据写入mysql数据保存
    reduceDS.foreachRDD(
      rdd => {

        rdd.foreachPartition(
          datas => {
            //建立数据库连接
            val connection: Connection = JDBCUtil.getConnection()
            val pstat: PreparedStatement = connection.prepareStatement(
              """
                insert into area_city_ad_count ( dt, area, city, adid, count )
                values ( ?, ?, ?, ?, ? )
                on duplicate key
                update count = count + ?
              """.stripMargin)
            datas.foreach{
              case ((dt,area,city,adid),count) => {
                pstat.setString(1,dt)
                pstat.setString(2,area)
                pstat.setString(3,city)
                pstat.setString(4,adid)
                pstat.setLong(5,count)
                pstat.setLong(6,count)
                pstat.executeUpdate()
              }
            }
            pstat.close()
            connection.close()
          }
        )
      }
    )
  }
}
