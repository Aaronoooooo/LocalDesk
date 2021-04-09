package com.atguigu.spark.day07.application

import com.atguigu.spark.day07.controller.{HotCategoryAnalysisTop10Controller, HotCategorySessionAnalysisTop10Controller}
import com.atguigu.summer.framework.core.TApplication

/**
 * * @create 2020-06-09 14:04
 */
object HotCategorySessionAnalysisTop10Application extends App with TApplication{

  // TODO 热门品类前10应用程序
  start("spark") {
    val controller = new HotCategorySessionAnalysisTop10Controller
    controller.execute()
  }
}
