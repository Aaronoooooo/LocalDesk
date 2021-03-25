package com.mobikok.bigdata.spark.core.req.controller

import com.mobikok.bigdata.spark.core.req.service.PageflowService
import com.mobikok.dev.framework.core.TController

class PageflowController extends TController{

    private val pageflowService = new PageflowService

    override def execute(): Unit = {
        val result = pageflowService.analysis()
    }
}
