package com.mobikok.bigdata.spark.streaming.req.service

import com.mobikok.bigdata.spark.streaming.req.bean.Ad_Click_Log
import com.mobikok.bigdata.spark.streaming.req.dao.LasHourAdCountAnalysisDaoTest
import com.mobikok.dev.framework.core.TService
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

/**
  * @author Aaron
  * @date 2020/07/29
  */
class LasHourAdCountAnalysisServiceTest extends TService {
  private val lasHourAdCountAnalysisDaoTest = new LasHourAdCountAnalysisDaoTest

  /**
    * 数据分析
    *
    * @return
    */
  override def analysis() = {
    val messageDS: DStream[String] = lasHourAdCountAnalysisDaoTest.readKafka()
    val logDS: DStream[Ad_Click_Log] = messageDS.map(
      log => {
        val datas: Array[String] = log.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    val tsToDS: DStream[((String, Long), Int)] = logDS.map(
      log => {
        val ts: Long = log.ts.toLong
        ((log.adid, ts / 10000 * 10000), 1)
      }
    )
    val windDS = tsToDS.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      Minutes(1),
      Seconds(10)
    )
    val adToTimeSumDS: DStream[(String, (Long, Int))] = windDS.map {
      case ((adid, time), sum) => {
        (adid, (time, sum))
      }
    }
    val groupDS: DStream[(String, Iterable[(Long, Int)])] = adToTimeSumDS.groupByKey()
    val resultDS: DStream[(String, List[(Long, Int)])] = groupDS.mapValues(
      iter => {
        iter.toList.sortWith(
          (l, r) => {
            l._1 < r._1
          }
        )
      }
    )
    resultDS.print
  }
}
