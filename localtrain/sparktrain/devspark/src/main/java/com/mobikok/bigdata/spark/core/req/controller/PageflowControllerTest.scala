package com.mobikok.bigdata.spark.core.req.controller

import com.mobikok.bigdata.spark.core.req.service.{PageflowServiceTest}
import com.mobikok.dev.framework.core.TController

class PageflowControllerTest extends TController{

    private val pageflowService = new PageflowServiceTest

    override def execute(): Unit = {
        val result = pageflowService.analysis()
    }
}
