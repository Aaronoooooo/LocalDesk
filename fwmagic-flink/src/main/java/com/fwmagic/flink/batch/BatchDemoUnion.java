package com.fwmagic.flink.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import scala.Tuple2;

import java.util.ArrayList;

public class BatchDemoUnion {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
        data1.add(Tuple2.apply(1,"zs"));
        data1.add(Tuple2.apply(2,"ls"));
        data1.add(Tuple2.apply(3,"ww"));


        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(Tuple2.apply(1,"zs123"));
        data2.add(Tuple2.apply(2,"ls123"));
        data2.add(Tuple2.apply(3,"ww123"));


        DataSource<Tuple2<Integer, String>> dataSource1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSource2 = env.fromCollection(data2);
        dataSource1.union(dataSource2).print();

    }
}
