package com.atguigu.spark.req.service

import com.atguigu.spark.req.dao.MockDataDao
import com.atguigu.summer.framework.core.TService

/**
 * @author zxjgreat
 * @create 2020-06-15 15:05
 */
class MockDataService extends TService {

  private val mockDataDao = new MockDataDao

  /**
   * 数据分析
   *
   * @return
   */
  override def analysis() = {

    val datas = mockDataDao.genMockData()

    // TODO 生成模拟数据
    //import mockDataDao._

    // TODO 向Kafka中发送数据
    mockDataDao.writeToKakfa(datas)


  }
}

