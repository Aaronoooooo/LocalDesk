package com.atguigu.spark.day07.application

import com.atguigu.spark.day07.controller.HotCategoryAnalysisTop10Controller
import com.atguigu.summer.framework.core.TApplication

/**
 * @author zxjgreat
 * @create 2020-06-09 14:04
 */
object HotCategoryAnalysisTop10Application extends App with TApplication{

  // TODO 热门品类前10应用程序
  start("spark") {
    val controller = new HotCategoryAnalysisTop10Controller
    controller.execute()
  }
}
