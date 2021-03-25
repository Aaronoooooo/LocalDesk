package com.mobikok.bigdata.spark.streaming.req.controller

import com.mobikok.bigdata.spark.streaming.req.service.BlackListServiceTest
import com.mobikok.dev.framework.core.TController

/**
  * @author Aaron
  * @date 2020/07/28
  */
class BlackListControllerTest extends TController{
  private val blackListServiceTest = new BlackListServiceTest
  override def execute(): Unit = {
    blackListServiceTest.analysis()
  }
}
