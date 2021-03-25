package com.mobikok.bigdata.spark.streaming.req.controller

import com.mobikok.bigdata.spark.streaming.req.service.MockDataService
import com.mobikok.dev.framework.core.TController

class MockDataController extends TController{

    private val mockDataService = new MockDataService

    override def execute(): Unit = {
        val result = mockDataService.analysis()
    }
}
