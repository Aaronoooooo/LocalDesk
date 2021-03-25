package com.mobikok.bigdata.spark.streaming.req.service

import java.text.SimpleDateFormat

import com.mobikok.bigdata.spark.streaming.req.bean.Ad_Click_Log
import com.mobikok.bigdata.spark.streaming.req.dao.DateAreaCityAdCountAnalysisDao
import com.mobikok.dev.framework.core.TService
import com.mobikok.dev.framework.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

class DateAreaCityAdCountAnalysisService extends TService{

    private val dateAreaCityAdCountAnalysisDao = new DateAreaCityAdCountAnalysisDao

    /**
      * 数据分析
      *
      * @return
      */
    override def analysis() = {

        // TODO 读取Kafka的数据
        val messageDS = dateAreaCityAdCountAnalysisDao.readKafka()

        // TODO 将数据转换为样例类来使用
        val logDS = messageDS.map(
            data => {
                val datas = data.split(" ")
                Ad_Click_Log( datas(0),datas(1),datas(2),datas(3),datas(4) )
            }
        )

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val dayDS = logDS.map(
            log => {
                val ts = log.ts
                val day = sdf.format(new java.util.Date(ts.toLong))
                (( day, log.area, log.city, log.adid ), 1)
            }
        )

        // TODO 将数据进行统计
        val resultDS: DStream[((String, String, String, String), Int)] =
            dayDS.reduceByKey(_+_)

        // TODO 将统计的结果保存到Mysql中
        resultDS.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    datas => {
                        // TODO 获取数据库的连接
                        val conn = JDBCUtil.getConnection()
                        val pstat = conn.prepareStatement(
                            """
                              |insert into area_city_ad_count ( dt, area, city, adid, count )
                              |values ( ?, ?, ?, ?, ? )
                              |on duplicate key
                              |update count = count + ?
                            """.stripMargin)

                        // TODO 操作数据库
                        datas.foreach{
                            case ( (dt, area, city, adid), count ) => {
                                pstat.setString(1, dt)
                                pstat.setString(2, area)
                                pstat.setString(3, city)
                                pstat.setString(4, adid)
                                pstat.setLong(5, count)
                                pstat.setLong(6, count)
                                pstat.executeUpdate()
                            }
                        }

                        // TODO 关闭资源
                        pstat.close()
                        conn.close()

                    }
                )
            }
        )
    }
}
