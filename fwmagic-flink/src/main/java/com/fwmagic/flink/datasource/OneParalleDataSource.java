package com.fwmagic.flink.datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义并行度为1的source
 * 模拟产生从1开始的数字
 */
public class OneParalleDataSource implements SourceFunction<Long> {

    private Long count = 1L;

    private Boolean isRunning = true;

    /**
     * 主要方法，这里循环产生数据，一秒一次
     *
     * @param sc
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> sc) throws Exception {
        while (isRunning) {
            sc.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
