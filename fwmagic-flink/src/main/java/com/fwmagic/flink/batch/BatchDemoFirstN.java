package com.fwmagic.flink.batch;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BatchDemoFirstN {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2, "zs"));
        data.add(new Tuple2<>(2, "ls"));
        data.add(new Tuple2<>(2, "bs"));
        data.add(new Tuple2<>(4, "ls"));
        data.add(new Tuple2<>(4, "ss"));
        data.add(new Tuple2<>(4, "as"));
        data.add(new Tuple2<>(3, "iw"));
        data.add(new Tuple2<>(3, "ww"));
        data.add(new Tuple2<>(3, "aw"));
        data.add(new Tuple2<>(1, "xw"));
        data.add(new Tuple2<>(1, "aw"));
        data.add(new Tuple2<>(1, "mw"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        //获取前3条，按插入集合的顺序排列
        //text.first(3).print();

        //对第一行进行分组，取前2个数据
        //text.groupBy(0).first(2).print();

        //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
        //text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();

        //不分组，全局排序:针对第一个元素升序，第二个元素倒序
        text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).print();
    }
}
