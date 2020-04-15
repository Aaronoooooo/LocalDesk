package com.fwmagic.flink.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class BatchDemoCounter {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> dataSource = env.fromElements("a", "b", "c", "d");

        DataSet<String> dataSet = dataSource.map(new RichMapFunction<String, String>() {

            IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("line-counter", this.counter);
            }

            @Override
            public String map(String value) {
                this.counter.add(1);
                return value;
            }
        }).setParallelism(4);


        dataSet.writeAsText("/Users/fangwei/counter.txt");

        JobExecutionResult execute = env.execute("counter demo");

        int result = execute.getAccumulatorResult("line-counter");
        System.out.println("result: " + result);
    }
}
