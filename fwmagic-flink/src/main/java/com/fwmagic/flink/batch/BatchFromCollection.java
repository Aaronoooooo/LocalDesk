package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LambdaUtil;

import java.util.Arrays;

public class BatchFromCollection {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.fromElements("hello", "thank you", "are you ok");

       /* DataSet<Tuple2<String, Long>> dataSet = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] arr = s.split("\\s");
                Arrays.stream(arr).forEach(x -> collector.collect(Tuple2.of(x, 1L)));
            }
        });*/

        /*dataSet.groupBy(0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        }).print();*/

        /**
         * flink中lambda表达式对FlatMap支持不太好，表现在不能自定判断返回值的类型
         * 需要再调用下returns方法来指定返回值的类型。
         */
        DataSet<Tuple2<String, Long>> dataSet = dataSource.flatMap((String v, Collector<Tuple2<String, Long>> collector) -> {
            String[] arr = v.split("\\s");
            for (String string : arr) collector.collect(Tuple2.of(string, 1L));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        dataSet.groupBy(0)
                .reduce((Tuple2<String, Long> t1, Tuple2<String, Long> t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .print();
    }
}