package com.mobikok.bigdata.spark.streaming.req.service

import com.mobikok.bigdata.spark.streaming.req.bean.Ad_Click_Log
import com.mobikok.bigdata.spark.streaming.req.dao.LasHourAdCountAnalysisDao
import com.mobikok.dev.framework.core.TService
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

class LasHourAdCountAnalysisService extends TService{

    private val lasHourAdCountAnalysisDao = new LasHourAdCountAnalysisDao

    /**
      * 数据分析
      *
      * @return
      */
    override def analysis() = {

        // TODO 读取kafka的数据
        val messageDS: DStream[String] = lasHourAdCountAnalysisDao.readKafka()

        // TODO 将数据转换为样例类使用
        val logDS = messageDS.map(
            data => {
                val datas = data.split(" ")
                Ad_Click_Log( datas(0),datas(1),datas(2),datas(3),datas(4) )
            }
        )
        // TODO 将数据进行结构的转换
        // (（adid, time）,1)
        // 10:01 => 10:00 ,1
        // 10:11 => 10:10, 1
        // 10:12 => 10:10, 1 => (10:10, 2)
        // 10:35 => 10:30
        // 10:55 => 10:50
        // 51000 => 51000 / 1000 = > 51 => 51/10=>5*10 => 50
        val tsToCountDS = logDS.map(
            log => {
                val ts = log.ts.toLong
                (( log.adid, ts / 10000 * 10000 ), 1)
            }
        )

        // TODO 将数据进行分组聚合(窗口)
        // (（adid, time）,1) => (（adid, time）,sum)
        val tsToSumDS = tsToCountDS.reduceByKeyAndWindow(
            (x:Int, y:Int) => x+y,
            Minutes(1),
            Seconds(10)
        )

        // TODO 将数据转换结构后根据广告进行分组
        // (（adid, time）,sum) => (adid, (time,sum))
        val adToTimeSumDS = tsToSumDS.map{
            case ( (adid, time), sum ) => {
                ( adid, (time, sum) )
            }
        }

        // (adid, (time,sum)) => (adid, Iterator[(time,sum)])
        val groupDS: DStream[(String, Iterable[(Long, Int)])] = adToTimeSumDS.groupByKey()

        // TODO 将分组后的数据进行时间排序，显示数据
        val resultDS = groupDS.mapValues(
            iter => {
                iter.toList.sortWith(
                    (left, right) => {
                        left._1 < right._1
                    }
                )
            }
        )

        resultDS.print()
    }
}
