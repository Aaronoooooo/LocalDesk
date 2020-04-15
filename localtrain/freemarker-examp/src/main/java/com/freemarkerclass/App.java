package com.freemarkerclass;


import com.freemarkerclass.generator.EntityGenerator;

/**
 * @author pengfeisu
 * @create 2020-02-12 14:13
 */

public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("开始启动---------------");
        String path = App.class.getClassLoader().getResource("").getPath();
        System.out.println(path);
        EntityGenerator generator = new EntityGenerator("pojo.ftl");
        try {
            generator.setSavePath("D:\\object\\itripljh\\itripbeans\\src\\main\\java\\pojo2");
            generator.generateCode();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
