package com.flinktableapi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class WordCountExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements(
                "Who's there?");
        DataSource<String> dataSource = env.readTextFile("C:\\Users\\supreme\\Desktop\\javascaladesk\\src\\main\\resources\\flume_dir_hdfs.ftl");
        MapOperator<String, String[]> map = dataSource.map(l -> l.split("\\W"));

        DataSource<String> input = env.fromElements(
                "I think I hear them. Stand, ho! Who's there?");


        data.map(new MapFunction<String, Integer>() {
            public Integer map(String value) {
                return Integer.parseInt(value);
            }
        });

        data.flatMap(new FlatMapFunction<String, LineSplitter>() {
            @Override
            public void flatMap(String value, Collector<LineSplitter> out) throws Exception {
                LineSplitter lineSplitter = new LineSplitter();
                HashMap<String, Object> map = new HashMap<>();
                map.put("user", lineSplitter.user);
                map.put("product", lineSplitter.product);
                map.put("amount", lineSplitter.amount);
                out.collect(lineSplitter);
            }
        });

        data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("Who's there?");
            }
        });

        data.join(input);

    }

    public static class LineSplitter {
        public Long user;
        public String product;
        public int amount;

        public LineSplitter() {
        }

        public LineSplitter(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "LineSplitter{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }

}