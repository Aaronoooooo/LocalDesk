package com.atguigu.spark.day07.controller

import com.atguigu.spark.day07.bean.bean
import com.atguigu.spark.day07.service.{HotCategoryAnalysisTop10Service, HotCategorySessionAnalysisTop10Service}
import com.atguigu.summer.framework.core.TController

/**
 * @author zxjgreat
 * @create 2020-06-09 14:28
 */
class HotCategorySessionAnalysisTop10Controller extends TController {

  private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service
  private val hotCategorySessionAnalysisTop10Service = new HotCategorySessionAnalysisTop10Service

  override def execute(): Unit = {
    val categories: List[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis()
    val result = hotCategorySessionAnalysisTop10Service.analysis(categories)
    result.foreach(println)
  }
}
