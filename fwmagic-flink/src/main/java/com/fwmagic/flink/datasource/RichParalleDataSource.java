package com.fwmagic.flink.datasource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 丰富的、多并发的自定义数据源，继承RichParallelSourceFunction
 */
public class RichParalleDataSource extends RichParallelSourceFunction<Long> {

    private static AtomicLong count = new AtomicLong(0);

    private static Boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("连接资源！");
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(count.incrementAndGet());
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        System.out.println("关闭资源！");
        return super.clone();
    }
}
