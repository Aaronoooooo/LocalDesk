package com.jsonmysql;

/**
 * @author pengfeisu
 * @create 2020-06-23 17:24
 */
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OracleJdbc {

    public static void main(String[] args) throws IOException {

        String db_url = OracleJdbc.getDbInfo().get("db_url");
        String db_user = OracleJdbc.getDbInfo().get("db_user");
        String db_pwd = OracleJdbc.getDbInfo().get("db_pwd");

        Connection conn = OracleJdbc.getConn(db_url, db_user, db_pwd);

        // String cmd = "create table test (id number(5), name varchar(10))";
        String cmd2 = "create table hr.stuinfo \n" +
                "(\n" +
                "  stuid      varchar2(11) not null,--学号：'S'+班号（7位数）+学生序号（3位数）\n" +
                "  stuname    varchar2(50) not null,--学生姓名\n" +
                "  sex        char(1) not null,--性别\n" +
                "  age        number(2) not null,--年龄\n" +
                "  classno    varchar2(7) not null,--班号：'C'+年级（4位数）+班级序号（2位数）\n" +
                "  stuaddress varchar2(100) default '地址未录入',--地址 \n" +
                "  grade      char(4) not null,--年级\n" +
                "  enroldate  date,--入学时间\n" +
                "  idnumber   varchar2(18) default '身份证未采集' not null--身份证\n" +
                ")\n" +
                "tablespace hr \n" +
                "  storage\n" +
                "  (\n" +
                "    initial 64K\n" +
                "    minextents 1\n" +
                "    maxextents unlimited\n" +
                "  )";

        OracleJdbc.updateCMD(conn, cmd2);
    }

    public static void updateCMD(Connection conn, String cmd) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(cmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static Connection getConn(String url, String user, String pwd) {

        Connection conn = null;

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Map<String, String> getDbInfo() throws IOException {
        Map<String, String> map = new HashMap<String, String>();

        Properties p = new Properties();
        InputStream in = Object.class.getResourceAsStream("/config.properties");

        try {
            p.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        map.put("db_user", p.getProperty("db_user"));
        map.put("db_pwd", p.getProperty("db_pwd"));
        map.put("db_url", p.getProperty("db_url"));

        return map;
    }
}