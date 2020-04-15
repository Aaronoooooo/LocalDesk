package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class BatchDemoDistinct {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> list = new ArrayList<>();
        list.add("hello a");
        list.add("hello b");

        DataSource<String> dataSource = env.fromCollection(list);

        DataSet<String> dataSet = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] arr = value.toLowerCase().split("\\W+");
                for (String line : arr) {
                    collector.collect(line);
                }
            }
        });

        dataSet.distinct().print();
    }
}
