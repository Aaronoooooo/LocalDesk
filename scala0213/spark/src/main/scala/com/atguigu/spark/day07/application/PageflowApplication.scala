package com.atguigu.spark.day07.application

import com.atguigu.spark.day07.controller. PageflowController
import com.atguigu.summer.framework.core.TApplication

/**
 * @author zxjgreat
 * @create 2020-06-10 10:01
 */
object PageflowApplication extends App with TApplication{

  start( "spark" ) {
    val controller = new PageflowController
    controller.execute()
  }
}
