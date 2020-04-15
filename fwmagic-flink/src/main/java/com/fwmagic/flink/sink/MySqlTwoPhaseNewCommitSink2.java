package com.fwmagic.flink.sink;

import com.alibaba.fastjson.JSON;
import com.fwmagic.flink.pojo.Student;
import com.fwmagic.flink.util.DBConnectUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 通过两阶段提交，实现消费的exactly-once(不多不少，仅消费一次)
 */
public class MySqlTwoPhaseNewCommitSink2 extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.err.println("start open……");
    }

    public MySqlTwoPhaseNewCommitSink2() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        connection = this.connection;
        System.err.println("start invoke.......");
        String stu = objectNode.get("value").toString();
        Student student = JSON.parseObject(stu, Student.class);
        String sql = "insert into student(id,name,password,age) values (?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setInt(1, student.getId());
        ps.setString(2, student.getName());
        ps.setString(3, student.getPassword());
        ps.setInt(4, student.getAge());
        ps.executeUpdate();
        //手动制造异常
        //if(Integer.parseInt(value) == 15) System.out.println(1/0);

    }

    @Override
    protected Connection beginTransaction() throws Exception {
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        this.connection = DBConnectUtil.getConnection(url, "root", "123456");
        System.err.println("start beginTransaction......."+this.connection);
        return this.connection;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {
        connection = this.connection;
        System.err.println("start preCommit......." + connection);
    }

    @Override
    protected void commit(Connection connection) {
        connection = this.connection;
        System.err.println("start commit......." + connection);
        try {
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void abort(Connection connection) {
        System.err.println("start abort rollback......." + this.connection);
        try {
            connection.rollback();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

