package com.mobikok.bigdata.spark.streaming.req.application

import com.mobikok.bigdata.spark.streaming.req.controller.{BlackListController, BlackListControllerTest}
import com.mobikok.dev.framework.core.TApplication

/**
  * @author Aaron
  * @date 2020/07/28
  */
object BlackListApplicationTest extends App with TApplication {
  start("sparkStreaming") {
    val controller = new BlackListControllerTest
    controller.execute()
  }

}
