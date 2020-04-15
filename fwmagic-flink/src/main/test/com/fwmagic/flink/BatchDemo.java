package com.fwmagic.flink;

import com.fwmagic.flink.util.DBConnectUtil;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class BatchDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSource<String> dataSource = env.readTextFile("file:///Users/fangwei/temp/time.txt");
        DataSet<Tuple3<String, String, Long>> dataSet = dataSource.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String line) throws Exception {
                String[] arr = line.split(",");
                return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
            }
        });
        DataSet<List<Tuple3<String, String, Long>>> ds = dataSet.groupBy(0, 2).sortGroup(1, Order.ASCENDING).reduceGroup(new GroupReduceFunction<Tuple3<String, String, Long>, List<Tuple3<String, String, Long>>>() {

            @Override
            public void reduce(Iterable<Tuple3<String, String, Long>> values, Collector<List<Tuple3<String, String, Long>>> out) throws Exception {
                List<Tuple3<String, String, Long>> result = new ArrayList<>();
                Iterator<Tuple3<String, String, Long>> iterator = values.iterator();
                List<Tuple3<String, String, Long>> list = IteratorUtils.toList(iterator);
                result.add(list.get(0));
                //todo foreach

                result.add(list.get(list.size() - 1));
                out.collect(result);
            }
        });

        DataSet<Tuple3<String, String, Long>> dds = ds.mapPartition(new MapPartitionFunction<List<Tuple3<String, String, Long>>, Tuple3<String, String, Long>>() {
            PreparedStatement ps = null;
            Connection connection = null;

            @Override
            public void mapPartition(Iterable<List<Tuple3<String, String, Long>>> values, Collector<Tuple3<String, String, Long>> out) throws Exception {
                String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
                connection = DBConnectUtil.getConnection(url, "root", "123456");

                Iterator<List<Tuple3<String, String, Long>>> it = values.iterator();
                while (it.hasNext()) {
                    List<Tuple3<String, String, Long>> list = it.next();
                    for (Tuple3<String, String, Long> t3 : list) {
                        out.collect(t3);
                        ps = connection.prepareStatement("insert into temp(`name`,`time`,`type`) values (?,?,?)");
                        ps.setString(1, t3.f0);
                        ps.setString(2, t3.f1);
                        ps.setLong(3, t3.f2);
                        ps.execute();
                    }
                }
                DBConnectUtil.close(connection);
            }
        });
        System.out.println(dds.count());
    }
}
