package com.atguigu.spark.req.application

import com.atguigu.spark.req.controller.MockDataController
import com.atguigu.summer.framework.core.TApplication

/**
 * * @create 2020-06-15 15:02
 */
object MockDataApplication extends App with TApplication{

  start("sparkStreaming") {
    val controller = new MockDataController
    controller.execute()
  }
}
