package com.jsonmysql;

import com.google.gson.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @author pengfeisu
 * @create 2020-03-12 9:39
 */
public class TestJsonDateToMysql {
    private static final Logger log = LoggerFactory.getLogger(TestJsonDateToMysql.class);
    private static final String url = "jdbc:mysql://flydiysz.cn:30331/default_server_datafactory?useUnicode=true&characterEncoding=utf8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC";
    private static final String user = "default_server_datafactory";
    private static final String password = "default_server_datafactory";
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
        moreinsertdata(con);
    }

    @Test
    /*UUID是指在一台机器上生成的数字,它保证对在同一时空中的所有机器都是唯一的
    * 标准的UUID格式为：xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (8-4-4-4-12)*/
    public void UUIDTest() {
        String uId = UUID.randomUUID().toString();
            System.out.println(uId);
            System.out.println(uId.replace("-",""));
    }
}
