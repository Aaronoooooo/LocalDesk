package com.mobikok.bigdata.spark.streaming.req.service

import com.mobikok.bigdata.spark.streaming.req.dao.MockDataDao
import com.mobikok.dev.framework.core.TService

class MockDataService extends TService{

    private val mockDataDao = new MockDataDao

    /**
      * 数据分析
      *
      * @return
      */
    override def analysis() = {
        // TODO 生成模拟数据
        //import mockDataDao._
       val datas: () => Seq[String] = mockDataDao.genMockData _
        //val a = Seq("a")

        // TODO 向Kafka中发送数据
        mockDataDao.writeToKakfa(datas)


    }
}
