package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BatchDemoBroadcast {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1、准备要广播的数据
        ArrayList<Tuple2<String,Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs", 23));
        broadData.add(new Tuple2<>("ls", 34));
        broadData.add(new Tuple2<>("ww", 45));

        //1.1 处理需要广播的数据，把数据集转成map,key为name，value为age
        DataSet<HashMap<String, Integer>> dateSet = env.fromCollection(broadData).map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> tuple) throws Exception {
                HashMap<String,Integer> hashMap = new HashMap<>();
                hashMap.put(tuple.f0, tuple.f1);
                return hashMap;
            }
        });

        //源数据
        DataSource<String> dataSource = env.fromElements("zs", "ls", "ww");

        //注意：在这里需要使用到RichMapFunction获取广播变量
        DataSet<String> result = dataSource.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadcastMap = new ArrayList<>();
            HashMap<String, Integer> allMap = new HashMap<>();


            @Override
            public void open(Configuration parameters) throws Exception {
                broadcastMap = getRuntimeContext().getBroadcastVariable("mybroadcastdata");
                for (HashMap<String, Integer> map : broadcastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String name) throws Exception {
                Integer age = allMap.get(name);
                return name + "," + age;
            }

        }).withBroadcastSet(dateSet, "mybroadcastdata");

        result.print();
    }
}
