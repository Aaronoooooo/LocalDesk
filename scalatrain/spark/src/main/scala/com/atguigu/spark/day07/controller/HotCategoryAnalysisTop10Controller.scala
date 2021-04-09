package com.atguigu.spark.day07.controller

import com.atguigu.spark.day07.service.HotCategoryAnalysisTop10Service
import com.atguigu.summer.framework.core.TController

/**
 * * @create 2020-06-09 14:28
 */
class HotCategoryAnalysisTop10Controller extends TController {

  private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service

  override def execute(): Unit = {
    val result = hotCategoryAnalysisTop10Service.analysis()

    result.foreach(println)
  }
}
