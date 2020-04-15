package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import scala.Tuple2$;

import java.util.ArrayList;

public class BatchDemoJoin {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));


        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(3, "guangzhou"));

        DataSource<Tuple2<Integer, String>> dataSource1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSource2 = env.fromCollection(data2);

        /*JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> operator = dataSource1.join(dataSource2).where(0)//指定第一个数据源的需要进行比较的元素角标
                .equalTo(0)//指定第二个数据源需要进行比较的元素角标
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return Tuple3.of(first.f0, first.f1, second.f1);
                    }
                });

        operator.print();*/

        DataSet<Tuple3<Integer, String, String>> dataSet = dataSource1.join(dataSource2).where(0).equalTo(0).map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> tuple) throws Exception {
                return Tuple3.of(tuple.f0.f0, tuple.f0.f1, tuple.f1.f1);
            }
        });

        dataSet.print();
        //  env.execute("data join");
    }
}
