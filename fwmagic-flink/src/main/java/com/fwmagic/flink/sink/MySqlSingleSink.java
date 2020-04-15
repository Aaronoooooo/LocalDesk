package com.fwmagic.flink.sink;

import com.fwmagic.flink.pojo.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySqlSingleSink extends RichSinkFunction<Student> {

    private Connection connection;

    private PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into Student(id,name,password,age) values (?,?,?,?)";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Student student, Context context) throws SQLException {
        ps.setInt(1, student.getId());
        ps.setString(2, student.getName());
        ps.setString(3, student.getPassword());
        ps.setInt(4, student.getAge());
        ps.executeUpdate();
        System.err.println("invoke running!!!");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) ps.close();
        if (connection != null) connection.close();
    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("====>get connection exception!" + e.getLocalizedMessage());
        }
        return connection;
    }
}
