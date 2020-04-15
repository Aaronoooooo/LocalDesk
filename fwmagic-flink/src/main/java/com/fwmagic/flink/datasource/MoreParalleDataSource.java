package com.fwmagic.flink.datasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class MoreParalleDataSource implements ParallelSourceFunction<Long> {

    private Long count =0L;

    private static Boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while(isRunning){
            sourceContext.collect(count);
            count++;
            //每秒生成一次
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
