package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchDemoHashRangePartition {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> list = new ArrayList<>();

        list.add(new Tuple2<>(1, "aaa"));
        list.add(new Tuple2<>(2, "bbb"));
        list.add(new Tuple2<>(3, "ccc"));
        list.add(new Tuple2<>(4, "ddd"));
        list.add(new Tuple2<>(5, "eee"));
        list.add(new Tuple2<>(6, "fff"));
        list.add(new Tuple2<>(7, "ggg"));
        list.add(new Tuple2<>(8, "hhh"));
        list.add(new Tuple2<>(9, "kkk"));


        DataSource<Tuple2<Integer, String>> dataSource = env.fromCollection(list);

      /*  dataSource.partitionByHash(0)
                .mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {

            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> it, Collector<Tuple2<Integer, String>> collector) throws Exception {
                Iterator<Tuple2<Integer, String>> iterator = it.iterator();
                while(iterator.hasNext()){
                    Tuple2<Integer, String> next = iterator.next();
                    System.out.println(Thread.currentThread().getId()+" : "+next);
                    //collector.collect(next);
                }
            }
        }).print();*/

        dataSource.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer,String>, Tuple2<Integer,String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
                Iterator<Tuple2<Integer, String>> it = iterable.iterator();
                while(it.hasNext()){
                    Tuple2<Integer, String> next = it.next();
                    System.out.println(Thread.currentThread().getId()+" : "+next);
                }
            }
        }).print();
    }
}
