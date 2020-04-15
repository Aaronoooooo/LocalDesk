package com.freemarkertest;

/**
 * @author pengfeisu
 * @create 2020-01-15 16:35
 */

import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 最常见的问题：
 * java.io.FileNotFoundException: xxx does not exist.
 * FreeMarker jar 最新的版本（2.3.23）提示 Configuration 方法被弃用
 * 代码自动生产基本原理：
 * 数据填充 freeMarker 占位符
 */

public class FreemarkerDemo {

    private static final String TEMPLATE_PATH = "src/main/resources/templates";
    private static final String RESOURCE_PATH = "src/main/java/com/freemarker/freemarkertest2";

    public static void main(String[] args) {
        // step1 创建freeMarker配置实例
        Configuration configuration = new Configuration(Configuration.VERSION_2_3_28);
        Writer out = null;
        Template template2 = null;
        User user = new User();
        user.setId(12);
        user.setName("freemarker");

        try {
            // step2 获取模版路径
            configuration.setDirectoryForTemplateLoading(new File(TEMPLATE_PATH));
            // step3 创建数据模型
            Map<String, Object> dataMap = new HashMap<String, Object>();
            dataMap.put("classPath", "com.freemarker.freemarkertest");
            dataMap.put("className", "AutoCodeDemo");
            dataMap.put("helloWorld", "通过简单的 <代码自动生产程序> 演示 FreeMarker的HelloWorld！+ test");

            // step4 加载模版文件
            template2 = configuration.getTemplate("hello.ftl");

            // step5 生成数据
            File docFile = new File(RESOURCE_PATH + "\\" + "AutoCodeDemo.java");
            //getParentFile()获取父路径src\main\java\com\freemarker\freemarkertest,判断是否存在,不存在则创建
            //getPath()获取全路径src\main\java\com\freemarker\freemarkertest\AutoCodeDemo.java
            if (!docFile.getParentFile().exists()) {
                docFile.getParentFile().mkdirs();
            }
            FileOutputStream fileOutputStream = new FileOutputStream(docFile);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
            //out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(docFile)));
            out = new BufferedWriter(outputStreamWriter);

            // step6 输出文件
            template2.process(dataMap, out);

            System.out.println("->->->->--- AutoCodeDemo.java 文件创建成功 ! ---<-<-<-<-");

            /************************************************************************************************/
            File config;
            config = new File(RESOURCE_PATH, user.getName() + ".txt");

//            System.out.println("getParentFile:" + config1.getParentFile());
//            System.out.println("getParent:" + config1.getParent());
//            System.out.println("getPath:" + config1.getPath());
//            System.out.println("getAbsolutePath:" + config1.getAbsolutePath() + " getAbsoluteFile:" + config1.getAbsoluteFile() + '\n');
//            System.out.println("getCanonicalPath:" + config1.getCanonicalPath() + " getCanonicalFile:" + config1.getCanonicalFile());
//            System.out.println("getPath:" + config1.getCanonicalPath());
//            System.out.println("getPath:" + config1.getPath());
            if (!config.exists()) {
                config.getParentFile().mkdirs();
                System.out.println("properties1创建成功");
            }
            template2 = configuration.getTemplate("config.ftl");
            out = new OutputStreamWriter(new FileOutputStream(config));
            template2.process(user, out);

            config = new File(RESOURCE_PATH, String.valueOf(user.getId()) + ".txt");
            if (!config.exists()) {
                config.getParentFile().mkdirs();
                System.out.println("properties2创建成功");
            }
            template2 = configuration.getTemplate("config.ftl");
            out = new OutputStreamWriter(new FileOutputStream(config));
            template2.process(user, out);

            File log4j = new File(RESOURCE_PATH, "log4j.properties");
            if (!log4j.exists()) {
                log4j.getParentFile().mkdirs();
            }
            File logback = new File(RESOURCE_PATH, "logback.xml");
            if (!logback.exists()) {
                logback.getParentFile().mkdirs();
            }

            template2 = configuration.getTemplate("log4j.ftl");
            out = new OutputStreamWriter(new FileOutputStream(log4j));
            template2.process(user, out);

            template2 = configuration.getTemplate("logback.ftl");
            out = new OutputStreamWriter(new FileOutputStream(logback));
            template2.process(user, out);

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
    }

}