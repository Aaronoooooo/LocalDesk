package com.fwmagic.flink.sink;

import com.fwmagic.flink.pojo.Student;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class MySqlBatchSink extends RichSinkFunction<List<Student>> {

    private Connection connection;

    private PreparedStatement ps;

    private BasicDataSource dataSource;

    /**
     * 只程序启动时初始化一次，可用于创建连接池，获取连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into Student(id,name,password,age) values (?,?,?,?)";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(List<Student> students, Context context) throws Exception {
        for(Student student :students){
            ps.setInt(1, student.getId());
            ps.setString(2, student.getName());
            ps.setString(3, student.getPassword());
            ps.setInt(4, student.getAge());
            ps.addBatch();
        }
        int[] counts = ps.executeBatch();
        System.err.println("===>成功插入了["+counts.length+"]条数据！");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) ps.close();
        if (connection != null) connection.close();
    }

    private Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");

        dataSource.setInitialSize(5);
        dataSource.setMaxTotal(10);
        dataSource.setMaxIdle(2);

        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            System.out.println("====>get connection: " + connection);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("====>get connection exception!" + e.getLocalizedMessage());
        }
        return connection;
    }
}
