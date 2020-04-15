package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class BatchDemoOperatorMysql {
    public static void main(String[] args) throws Exception {

        String driverClass = "com.mysql.jdbc.Driver";
        String dbUrl = "jdbc:mysql://localhost:3306/test";
        String userNmae = "root";
        String passWord = "123456";
        String sql = "insert into test.temp (name,time,type) values (?,?,?)";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
         * 文件内容：
         * 关羽,2019-10-14 00:00:01,1
         * 张飞,2019-10-14 00:00:02,2
         * 赵云,2019-10-14 00:00:03,3
         */
        //写mysql
        String filePath = "/Users/fangwei/temp/data.csv";

        //读csv文件内容,转成Row对象
        DataSet<Row> outputData = env.readCsvFile(filePath).fieldDelimiter(",").types(String.class, String.class, Long.class).map(new MapFunction<Tuple3<String, String, Long>, Row>() {
            @Override
            public Row map(Tuple3<String, String, Long> t) throws Exception {
                Row row = new Row(3);
                row.setField(0, t.f0.getBytes("UTF-8"));
                row.setField(1, t.f1.getBytes("UTF-8"));
                row.setField(2, t.f2.longValue());
                return row;
            }
        });

        //将Row对象写到mysql
        outputData.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driverClass)
                .setDBUrl(dbUrl)
                .setUsername(userNmae)
                .setPassword(passWord)
                .setQuery(sql)
                .finish());


        //触发执行
        env.execute("insert data to mysql");

        System.out.println("mysql写入成功！");

        TimeUnit.SECONDS.sleep(6);

        //读mysql
        DataSource<Row> dataSource = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverClass)
                .setDBUrl(dbUrl)
                .setUsername(userNmae)
                .setPassword(passWord)
                .setQuery("select * from temp")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
                .finish());


        //获取数据并打印
        dataSource.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row value) throws Exception {
                System.out.println(value);
                return value.toString();
            }
        }).print();

    }
}
