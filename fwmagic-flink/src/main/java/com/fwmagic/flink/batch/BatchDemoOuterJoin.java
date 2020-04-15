package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class BatchDemoOuterJoin {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));
        data1.add(new Tuple2<>(4, "zl"));


        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(3, "guangzhou"));
        data2.add(new Tuple2<>(5, "gege"));

        DataSource<Tuple2<Integer, String>> dataSource1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSource2 = env.fromCollection(data2);

        /**
         * 左外连接
         */
       /* dataSource1.leftOuterJoin(dataSource2).where(0)
                .equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (second == null) {
                    return Tuple3.of(first.f0, first.f1, null);
                } else {
                    return Tuple3.of(first.f0, first.f1, second.f1);

                }
            }
        }).print();*/


        System.out.println("==============");


        /**
         * 右外连接
         */

       /* dataSource1.rightOuterJoin(dataSource2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return Tuple3.of(second.f0, null, second.f1);
                        } else {
                            return Tuple3.of(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();*/

        /**
         * fullOuterJoin,左右两边都可能为空
         */
        dataSource1.fullOuterJoin(dataSource2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return Tuple3.of(second.f0, null, second.f1);
                        } else if (second == null) {
                            return Tuple3.of(first.f0, first.f1, null);
                        } else {
                            return Tuple3.of(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();

    }
}
