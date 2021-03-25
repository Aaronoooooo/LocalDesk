package com.atguigu.spark.day07.controller

import com.atguigu.spark.day07.service.{HotCategoryAnalysisTop10Service, PageflowService}
import com.atguigu.summer.framework.core.TController

/**
 * @author zxjgreat
 * @create 2020-06-09 14:28
 */
class PageflowController extends TController{

  private val pageflowService = new PageflowService

  override def execute(): Unit = {
    val result = pageflowService.analysis()
  }
}

