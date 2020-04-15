package com.fwmagic.flink.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;
import java.util.List;

/**
 * 两个流的笛卡尔积
 */
public class BatchDemoCross {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> list1 = new ArrayList<>();
        list1.add("aaa");
        list1.add("bbb");
        list1.add("ccc");

        List<String> list2 = new ArrayList<>();
        list2.add("111");
        list2.add("222");
        list2.add("333");

        DataSource<String> dataSource1 = env.fromCollection(list1);
        DataSource<String> dataSource2 = env.fromCollection(list2);

        //两个流做笛卡尔积
        CrossOperator.DefaultCross<String, String> cross = dataSource1.cross(dataSource2);

        cross.print();
    }
}
