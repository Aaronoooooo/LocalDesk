package com.atguigu.spark.req.controller

import com.atguigu.spark.req.service.MockDataService
import com.atguigu.summer.framework.core.TController

/**
 * * @create 2020-06-15 15:04
 */
class MockDataController extends TController{

  private val mockDataService = new MockDataService

  override def execute(): Unit = {
    val result = mockDataService.analysis()
  }
}
