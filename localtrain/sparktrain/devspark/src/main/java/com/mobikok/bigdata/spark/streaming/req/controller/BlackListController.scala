package com.mobikok.bigdata.spark.streaming.req.controller

import com.mobikok.bigdata.spark.streaming.req.service.BlackListService
import com.mobikok.bigdata.spark.streaming.req.service.BlackListService
import com.mobikok.dev.framework.core.TController

class BlackListController extends TController{

    private val blackListService = new BlackListService

    override def execute(): Unit = {
        val result = blackListService.analysis()
    }
}
