package com.entity;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args) throws Exception {
        //测试数据
        List<MysqlTable> list = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            MysqlTable table = new MysqlTable();
            table.settName("sys_user" + i);

            List<MysqlColumn> list1 = new ArrayList<>();
            for (int j = 0; j < 1; j++) {
                MysqlColumn column = new MysqlColumn();
                column.setColumn("createTime" + j);
                column.setType("TEXT");
                list1.add(column);
            }
            table.setColNames(list1);
            list.add(table);
        }
        System.out.println("测试数据为：" + list);

        //有多少个表，就循环几次
        for (MysqlTable mysqlTable : list) {
            new test().gen(mysqlTable);
        }

    }

    public void gen(MysqlTable mysqlTable) throws IOException, TemplateException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(test.class, "/scripts");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        Template temp = cfg.getTemplate("Java_Entity.ftl");

        JavaEntityModel model = new JavaEntityModel();
        model.setClassName(NameConvert.entityName(mysqlTable.gettName()));

        List<MysqlColumn> colNames = mysqlTable.getColNames();
        for (MysqlColumn colName : colNames) {
            String columnName = colName.getColumn();
            String fieldName = NameConvert.fieldName(columnName);

            String columnType = colName.getType();
            ColumnFieldTypeMapping mapping = new ColumnFieldTypeMapping();
            Class fieldClass = mapping.getFieldType(columnType);
            if (fieldClass == null) {
                throw new UnsupportedOperationException("没有对应的映射类型");
            }
            model.addfield(fieldName, fieldClass);
            model.addImport(fieldClass.getName());
        }
        model.setPackageName("cn.flydiy");
        System.out.println("javaModel::" + model);

        File dir1 = new File("automodule/src/main/java");
        if (!dir1.exists()) {
            dir1.mkdirs();
        }
        File dir2 = new File("automodule/src/main/resource");
        if (!dir2.exists()) {
            dir2.mkdirs();
        }

//        OutputStream fos = new FileOutputStream(new File(dir1, model.getClassName() + ".java")); //java文件的生成目录
//        Writer out = new OutputStreamWriter(fos);
//        temp.process(model, out);

//        fos.flush();
//        fos.close();

        System.out.println("gen code success!");
    }
}
