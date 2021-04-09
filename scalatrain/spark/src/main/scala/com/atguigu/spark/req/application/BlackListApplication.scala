package com.atguigu.spark.req.application

import com.atguigu.spark.req.controller.BlackListController
import com.atguigu.summer.framework.core.TApplication

/**
 * * @create 2020-06-15 15:03
 */
object BlackListApplication extends App with TApplication{

  start("sparkStreaming") {
    val controller = new BlackListController
    controller.execute()
  }
}
