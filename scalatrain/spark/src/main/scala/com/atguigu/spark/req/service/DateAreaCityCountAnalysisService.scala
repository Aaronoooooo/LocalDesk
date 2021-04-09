package com.atguigu.spark.req.service

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.spark.req.bean.bean.Ad_Click_Log
import com.atguigu.spark.req.dao.DateAreaCityCountAnalysisDao
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream


/**
 * * @create 2020-06-16 8:58
 */
class DateAreaCityCountAnalysisService extends TService{

  private val dateAreaCityCountAnalysisDao = new DateAreaCityCountAnalysisDao

  /**
   * 数据分析
   *
   * @return
   */
  override def analysis() = {

    //读取Kafka的数据
    val messageDS = dateAreaCityCountAnalysisDao.readKafka()
    //将数据转换为样例类
    val logDS: DStream[Ad_Click_Log] = messageDS.map(
      data => {
        val datas: Array[String] = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //(key,1)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayDS: DStream[((String, String, String, String), Int)] = logDS.map(
      log => {
        val date = new Date(log.ts.toLong)
        val day: String = sdf.format(date)
        ((day, log.area, log.city, log.adid), 1)

      }
    )
    //聚合
    val reduceDS: DStream[((String, String, String, String), Int)] = dayDS.reduceByKey(_+_)
    //保存到mysql
    reduceDS.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          data => {
            //获取连接
            val connection: Connection = JDBCUtil.getConnection()
            val statement: PreparedStatement = connection.prepareStatement(
              """
                |insert into area_city_ad_count(dt, area, city, adid, count )
                |values (? ,? ,? ,? ,?)
                |on duplicate key
                |update count = count + ?
                |""".stripMargin

            )
            data.foreach{
              case ((dt, area, city, adid), count)=>{
                statement.setString(1,dt)
                statement.setString(2,area)
                statement.setString(3,city)
                statement.setString(4,adid)
                statement.setLong(5,count)
                statement.setLong(6,count)
              }
            }

            //关闭资源
            statement.close()
            connection.close()
          }
        )
      }
    )


  }
}
