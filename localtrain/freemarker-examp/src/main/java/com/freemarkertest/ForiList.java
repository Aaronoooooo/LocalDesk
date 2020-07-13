package com.freemarkertest;

import com.alibaba.fastjson.JSONObject;
import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.*;
import java.util.*;

/**
 * @author pengfeisu
 * @create 2020-06-09 17:21
 */
public class ForiList {
    public static void main(String[] args) {
        Configuration cfg = CodeProduce.cfg;
        Writer out = null;
        try {
            cfg.setDirectoryForTemplateLoading(new File("freemarker-examp/src/main/resources/templates"));
            // cfg.setClassForTemplateLoading(ForiList.class,"/templates");
            HashMap<String, String> map = new HashMap<>();
            // foriList数据集
            map.put("list", "userList");

            // ifList数据集
            map.put("ifi", "张三三");

            Template template = cfg.getTemplate("foriList.ftl");
            File file = new File("freemarker-examp/src/main/autocode.java");

            OutputStreamWriter outWriter = new OutputStreamWriter(new FileOutputStream(file));
            out = new BufferedWriter(outWriter);
            // step6 输出文件
            template.process(map, out);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != out) {
                    out.flush();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        // String templateSource = "crevariable.ftl";
        // String generateCode = "freemarker-examp/src/main/methodCode.java";
        String templateSource = "chavariabletest.ftl",
                generateCode = "freemarker-examp/src/main/methodCodeTest.java",
                variabJson2 = "{\"varCreateName\":\"list\",\n" +
                        "\"varCreateType\":\"String\",\n" +
                        "\"varCreateValue\":[\"张三\",\"李四\",\"王五\",\"赵六\"],\n" +
                        "\"varCreateOutName\":\"list\",\n" +
                        "\"varChangeName\":\"list\",\n" +
                        "\"varChangeType\":\"String\",\n" +
                        "\"varChangeValue\":[\"孙九\"],\n" +
                        "\"varChangeOutName\":\"changeList\",\n" +
                        "\"varIfMethod1\":\"isEmpty()\",\n" +
                        "\"varIfMethod2\":\"equals\",\n" +
                        "\"varIfOutName\":\"str\",\n" +
                        "\"varExeMethod\":[\"remove\"],\n" +
                        "\"methResulValue\":\"List\",\n" +
                        "\"methResulType\":\"String\",\n" +
                        "\"methName\":\"autoCodeTest\",\n" +
                        "\"methNameInParamName\":\"in\",\n" +
                        "\"methNameInParamType\":\"String\"}",
                variabJson = "{\"varCreateName\":\"list\",\n" +
                        "\"varCreateType\":\"String\",\n" +
                        "\"varCreateValue\":[\"张三\",\"李四\",\"王五\",\"赵六\"],\n" +
                        "\"varCreateOutName\":\"list\",\n" +
                        "\"varChangeName\":\"list\",\n" +
                        "\"varChangeType\":\"String\",\n" +
                        "\"varChangeValue\":[\"田七\",\"孙九\"],\n" +
                        "\"varChangeOutName\":\"changeList\",\n" +
                        "\"varIfMethod1\":[\"isEmpty()\",\"equals\"],\n" +
                        "\"varIfMethod2\":\"equals\",\n" +
                        "\"varIfOutName\":\"str\",\n" +
                        "\"varExeMethod\":[\"remove\"],\n" +
                        "\"methResulValue\":\"List\",\n" +
                        "\"methResulType\":\"String\",\n" +
                        "\"methName\":\"autoCodeTest\",\n" +
                        "\"methNameInParamName\":\"in\",\n" +
                        "\"methNameInParamType\":\"String\"}";;
        VarObject varObject = JSONObject.parseObject(variabJson, VarObject.class);
        System.out.println(varObject);
        HashMap<String, Object> map = new HashMap<>();
        map.put("var",varObject);
        //创建变量,修改变量,创建类型,修改类型,输出类型
        // map.put("varCreateName", "list");
        // map.put("varCreateType", "String");
        // map.put("varCreateValue", "张三");
        // map.put("varCreateOutName", "list");
        // map.put("varChangeName", "list");
        // map.put("varChangeType", "String");
        // map.put("varChangeValue", "李四");
        // map.put("varChangeOutName", "changeList");
        // map.put("varIfMethod1","isEmpty");
        // map.put("varIfMethod2","equals");
        // map.put("varIfOutName","str");
        // map.put("varExeMethod","remove");
        // map.put("methResulValue","List");
        // map.put("methResulType","String");
        // map.put("methName","autoCodeTest");
        // map.put("methNameInParamName","in");
        // map.put("methNameInParamType","String");
        String state = variableTest(map, templateSource, generateCode);
        System.out.println(state);
    }

    public static String variableTest(Map map, String templateSource, String generateCode) {
        Writer out = null;
        try {
            CodeProduce.cfg.setDirectoryForTemplateLoading
                    (new File("freemarker-examp/src/main/resources/templates"));
            Template template = CodeProduce.cfg.getTemplate(templateSource);
            File file = new File(generateCode);
            OutputStreamWriter outWriter = new OutputStreamWriter(new FileOutputStream(file));
            out = new BufferedWriter(outWriter);

            template.process(map, out);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != out) {
                    out.flush();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return generateCode + "执行完成!";
    }

    public Map ifList() {
        Map<Integer, Object> users = new HashMap<>();
        users.put(10, "张三");
        users.put(20, "李四");
        users.put(30, "王五");
        return users;
    }
    // 模版原型
    // for (int i = 0; i < list.size(); i++) {
    //     Integer in = list.get(i);
    //     if (in.equals(10)) {
    //         user.setName("张三三");
    //     } else if (in.equals(20)) {
    //         user.setName("李四四");
    //     } else if(in.equals(30)){
    //         user.setName("王五五");
    //     }else {
    //         System.out.println("年龄不匹配");
    //     }
    // }
// @Test
// public void testList1() {
//
// //开始(创建List)
//     List<String> list = new ArrayList<String>();
//     list.add("张三");
//
//     // 判断
//     if (!list.isEmpty()) {
//
//         //循环
//         for (int i = 0; i < list.size(); i++) {
//             String str = list.get(i);
//
//             // 判断
//             if (str.equals("李四")) {
//                 list.remove(i);
//             }
//         }
//     }
//     //结束
//     System.out.println(list.toString());
// }
}