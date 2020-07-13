package com.jsonmysql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/**
 * @author pengfeisu
 * @create 2020-03-12 9:39
 */
public class TestJsonDateToMysql {
    private static final Logger log = LoggerFactory.getLogger(TestJsonDateToMysql.class);
    //    private static final String url = "jdbc:mysql://flydiysz.cn:30333/default_server_datafactory?useUnicode=true&characterEncoding=utf8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC";
//    private static final String user = "default_server_datafactory";
//    private static final String password = "default_server_datafactory";
    private static final String url = "jdbc:mysql://flydiysz.cn:30333";
    private static final String user = "canal";
    private static final String password = "canal";
    private static Connection con;

    static Connection getconnect() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }

    public static void moreinsertdata(Connection con)//插入数据进入数据库中
    {
        JsonParser parser = new JsonParser();
        JsonObject object;
        try {
            object = (JsonObject) parser.parse(new FileReader("src/main/resources/jsondata/testdate.json"));
            JsonArray array = object.get("rows").getAsJsonArray();
            for (int i = 0; i < array.size(); i++) {
                JsonObject arrayObject = array.get(i).getAsJsonObject();
                PreparedStatement psql = con.prepareStatement("insert into xxx (number,name,floor,position,starttime,id)" + "values(?,?,?,?,?,?)");

                psql.setInt(1, arrayObject.get("number").getAsInt());
                psql.setString(2, arrayObject.get("name").getAsString().toString());
                psql.setString(3, arrayObject.get("floor").getAsString().toString());
                psql.setString(4, arrayObject.get("position").getAsString().toString());
                //解析json中出现时间格式的数值插入到mysql
                String date = arrayObject.get("starttime").getAsString().toString();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date myDate = dateFormat.parse(date);
                Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String str = format.format(myDate);
                psql.setString(5, str);
                String id = UUID.randomUUID().toString().replace("-", "");
                psql.setString(6, id);
                psql.executeUpdate();
                log.info(psql + "数据插入完成");
                psql.close();
            }
        } catch (JsonIOException e1) {
            e1.printStackTrace();
        } catch (JsonSyntaxException e1) {
            e1.printStackTrace();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Connection con = TestJsonDateToMysql.getconnect();
        try {
            Map<String, List<String>> allSchemaTables = TestJsonDateToMysql.getAllSchemaTables(con);
            for (Map.Entry<String, List<String>> entry : allSchemaTables.entrySet()) {
                System.out.println("dBname = " + entry.getKey() + ", tableName = " + entry.getValue());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static Map<String, List<String>> getAllSchemaTables(Connection conn) throws Exception {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet rsCatalog = databaseMetaData.getCatalogs();
        ResultSet rsTable = null;
        try {
            while (rsCatalog.next()) {
                String catalog = rsCatalog.getString(1);
                if (!catalog.equals("information_schema") && !catalog.equals("performance_schema") && !catalog.equals("mysql")) {
//                    list.add(schemaName);
                    List<String> tables = new ArrayList<String>();
                    String[] t = {"TABLE"};
                    rsTable = databaseMetaData.getTables(catalog, null, "%", t);
                    while (rsTable.next()) {
                        String table = rsTable.getString(3);

                        tables.add(table);
                    }
                    map.put(catalog, tables);
                }
            }
        } finally {
            conn.close();
        }
        return map;
    }

    /*UUID是指在一台机器上生成的数字,它保证对在同一时空中的所有机器都是唯一的
     * 标准的UUID格式为：xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (8-4-4-4-12)*/
    @Test
    public void UUIDTest() {
        String uId = UUID.randomUUID().toString();
        System.out.println(uId);
        System.out.println(uId.replace("-", ""));
    }

    @Test
    public void getMySQLSchema() throws SQLException {
        Connection con = TestJsonDateToMysql.getconnect();
        DatabaseMetaData metaData = con.getMetaData();
        ResultSet catalogs = metaData.getCatalogs();

        List<Object> list = new ArrayList<>();
        while (catalogs.next()) {
            String schemaName = catalogs.getString("TABLE_CAT");
            // skip internal schemas
            if (!schemaName.equals("information_schema") && !schemaName.equals("performance_schema") && !schemaName.equals("mysql")) {
                list.add(schemaName);
            }
        }
//        return list;
        for (Object o : list) {
            System.out.println(o.toString());
        }

    }

    @Test
    public void getMysqlTableValue() throws SQLException {
        String dataBaseName = "test";
        String tableName = "usermodule";
        JSONArray jsonArray = new JSONArray();
        //查询
        Connection con = TestJsonDateToMysql.getconnect();
        String sql = "select * from " + dataBaseName + "." + tableName;
        PreparedStatement statement = con.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();

        //获取列数
        ResultSetMetaData metaData = statement.getMetaData();
        int columnCount = metaData.getColumnCount();

        //获取表数据
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
//                if (i > 1) System.out.println();
                String columnName = metaData.getColumnName(i);
                String columnValue = resultSet.getString(columnName);
                System.out.print(columnValue + " " + columnName);
                System.out.println();
//                JSONObject jsonObject = new JSONObject();
                jsonObject.put(columnName, columnValue);
            }
            jsonArray.add(jsonObject);
        }
//            System.out.println(jsonObject);
        System.out.println(jsonArray.toJSONString());
//            String mysqlHost = MapUtils.getString(paramMap, "mysqlHost");
//            Integer mysqlPort = MapUtils.getInteger(paramMap, "mysqlPort");
//            String mysqlUserName = MapUtils.getString(paramMap, "mysqlUserName");
//            String mysqlPassword = MapUtils.getString(paramMap, "mysqlPassword");
//            List<MysqlDatabase> mysqlDatabases = new ArrayList<>();
//            MysqlUtil mysqlUtil = new MysqlUtil(mysqlHost, mysqlPort, mysqlUserName, mysqlPassword);


//            for (int i = 0; i < columnCount; i++) {
//                String columnClassName = metaData.getColumnClassName(i);
//                String value = rs.getString(columnClassName);
//                jsonObject.put(columnClassName, value);
//            }
        statement.close();
        con.close();
    }
}