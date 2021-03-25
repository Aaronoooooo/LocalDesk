package com.atguigu.spark.req.controller

import com.atguigu.spark.req.service.{BlackListService, MockDataService}
import com.atguigu.summer.framework.core.TController

/**
 * @author zxjgreat
 * @create 2020-06-15 15:04
 */
class BlackListController extends TController{

  private val blackListService = new BlackListService

  override def execute(): Unit = {
    val result = blackListService.analysis()
  }
}
