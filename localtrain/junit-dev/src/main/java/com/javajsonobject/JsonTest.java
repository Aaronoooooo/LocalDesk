package com.javajsonobject;

/**
 * @author pengfeisu
 * @create 2020-02-29 13:44
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2017/4/21.
 */
public class JsonTest {

    public static void main(String[] args) throws Exception {
        String str = "";
        /**
         * [
         {
         "data ": [
         {
         "building_id ": "*** ",
         "building_num ": "** ",
         "door_name ": "** ",
         "electric ": "** ",
         "room_name ": "** "
         }
         ],
         "success ": true
         }
         ]
         */
        System.out.println("************************************************************************");
//        String s = "[{\"success\":true,\"data\":[{\"building_id\":\"building_id\",\"building_num\":\"building_num\",\"room_name\":\"**\",\"door_name\":\"**\",\"electric\":\"**\"}]}]";
        String s = "[{\"success\":true,\"data\":[{\"building_id\":\"building_id\",\"building_num\":\"building_num\",\"room_name\":\"**\",\"door_name\":\"**\",\"electric\":\"**\"}]}]";
        String b = s.substring(0, s.length());
        String c = b.substring(1, b.length() - 1);
        System.out.println("b____" + b);
        System.out.println("c____" + c);
        JSONObject jsonx = JSON.parseObject(c);
        System.out.println("success:" + jsonx.getString("success"));
        JSONArray ja = jsonx.getJSONArray("data");
        for (int i = 0; i < ja.size(); i++) {
            JSONObject jo = ja.getJSONObject(i);
            System.out.println("building_id: " + jo.getString("building_id") + "\nbuilding_num: " + jo.getString("building_num"));
        }
//        //第二种格式
//        /**
//         * [
//         {
//         "password ": "*1234567890 ",
//         "success ": "true "
//         }
//         ]
//         */
//
//        s = "[{\"success\":\"true\",\"password\":\"*1234567890\"}]";
//        b = s.substring(0, s.length() - 1);
//        c = b.substring(1, b.length());
//        System.out.println(c + "c___");
//        JSONObject reagobj = JSONObject.fromObject(c);
//        String name = reagobj.getString("password");
//        System.out.println(name + "name,,,,,,");
//        String password = jm.getString("password");
//        System.out.println(password);
//        System.out.println("看看有没有值" + password);

        //第三种格式
        /**
         * {
         "data ": {
         "access_token ": "5a7040ccf66bafd06acd39b6f6",
         "expires_second ": 36000
         },
         "rlt_code ": "HH0000 ",
         "rlt_msg ": "成功 "
         }
         */

        String res = "{\"data\":{\"access_token\":\"5a7040ccf66bafd06\",\"expires_second\":36000},\"rlt_code\":\"HH0000\",\"rlt_msg\":\"成功\"}";
        JSONObject jsonObject = JSON.parseObject(res);
        String data = jsonObject.getString("data");
        System.out.println("res_data" + data);
        JSONObject jsondata = JSON.parseObject(data);
        System.out.println("access_token: " + jsondata.getString("access_token"));
        System.out.println("expires_second: " + jsondata.getString("expires_second"));


        //第四种格式
        /**
         * {
         "data ":
         {
         "total ":23,
         "start ":0,
         "total_page ":3,
         "rows ":
         [
         { "op_way ": "3 ", "user_mobile ": "15321918571 ", "op_time ":1493881391976, "pwd_no ":30},
         { "op_way ": "1 ", "op_time ":1493880995000, "pwd_no ":31}
         ],
         "current_page ":1,
         "page_size ":10
         },
         "rlt_code ": "HH0000 ",
         "rlt_msg ": "成功 "
         }
         */

        str = "{\"data\":{\"total\":23,\"start\":0,\"total_page\":3,\"rows\":[{\"op_way\":\"1\",\"op_time\":1493884964000,\"pwd_no\":31},{\"op_way\":\"3\",\"user_mobile\":\"18518517491\",\"op_time\":1493884615032,\"pwd_no\":30},{\"op_way\":\"3\",\"user_mobile\":\"18518517491\",\"op_time\":1493883836552,\"pwd_no\":30},{\"op_way\":\"1\",\"op_time\":1493883294000,\"pwd_no\":31},{\"op_way\":\"1\",\"op_time\":1493883256000,\"pwd_no\":31},{\"op_way\":\"3\",\"user_mobile\":\"15321918571\",\"op_time\":1493883015371,\"pwd_no\":30},{\"op_way\":\"1\",\"op_time\":1493882007000,\"pwd_no\":31},{\"op_way\":\"3\",\"user_mobile\":\"15321918571\",\"op_time\":1493881498520,\"pwd_no\":30},{\"op_way\":\"3\",\"user_mobile\":\"15321918571\",\"op_time\":1493881391976,\"pwd_no\":30},{\"op_way\":\"1\",\"op_time\":1493880995000,\"pwd_no\":31}],\"current_page\":1,\"page_size\":10},\"rlt_code\":\"HH0000\",\"rlt_msg\":\"成功\"}";
        jsonObject = JSON.parseObject(str);
        data = jsonObject.getString("data");
        JSONObject jsonObjects = JSON.parseObject(data);
        ja = jsonObjects.getJSONArray("rows");
        for (int i = 0; i < ja.size(); i++) {
            JSONObject jo = ja.getJSONObject(i);
            String op_way = jo.getString("op_way");
            String op_time = jo.getString("op_time");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long lt = new Long(op_time);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            String pwd_no = jo.getString("pwd_no");
            String user_mobile = jo.getString("user_mobile");
            System.out.println(op_way + res + pwd_no + user_mobile + "------------");
        }
    }

}
