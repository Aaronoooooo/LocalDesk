package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

public class BatchDemoMapPartition {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.fromCollection(Arrays.asList("aaa", "bbb", "ccc", "aaa"));


       /* DataSet<String> dataSet = dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                //每条数据来调用一次
                System.out.println("====每条数据来调用一次===");
                return "T_"+s;
            }
        });*/

        DataSet<String> dataSet = dataSource.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                System.out.println("一个分区的数据调用一次");
                Iterator<String> it = iterable.iterator();
                while(it.hasNext()){
                    String[] arr = it.next().split("\\W+");
                    for(String value:arr){
                        collector.collect("t_"+value);
                    }
                }
            }
        });

        dataSet.print();
    }
}
