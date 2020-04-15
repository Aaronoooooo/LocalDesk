package cn.flydiy.module;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class test {
    public static void main(String[] args) throws Exception {
        //测试数据
        List<MysqlTable> list = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            MysqlTable table = new MysqlTable();
            table.settName("Az_assign_appointment"+i);
            List<MysqlColumn> list1 = new ArrayList<>();
            for (int j = 0; j < 1; j++) {
                MysqlColumn column = new MysqlColumn();
                column.setColumn("createTime"+j);
                column.setType("TEXT");
                list1.add(column);
            }
            table.setColNames(list1);
            list.add(table);
        }
        System.out.println("测试数据为："+list);

        //有多少个表，就循环几次
        for (MysqlTable mysqlTable : list) {
            new test().genScalaModule(mysqlTable);
        }
        new test().genSingleTableFunction("Az_assign_appointment");

    }

    public void genMainFunction(MysqlTable mysqlTable) throws IOException, TemplateException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(test.class,"/scripts");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        Template temp = cfg.getTemplate("main.ftl");
        String moduleName = NameConvert.entityName(mysqlTable.gettName());
        Map<String, Object> map = new HashMap<>();
        map.put("packageName", "cn.flydiy.scala.evaluate");
        map.put("className", moduleName+"Function");

        List<MainSink> sinks = new ArrayList<>();

        MainSink sink = new MainSink();
        sink.setName(moduleName.toLowerCase()+"Stream");
        sink.setModuleName(moduleName);
        sink.setConstantName(moduleName);
        sink.setBuilderName(moduleName.toLowerCase()+"Builder");
        sink.setEsIndex(moduleName);
        sink.setModuleColumn("pgguid");
        sink.setStreamName(moduleName);
        sink.setKafkaTopic(moduleName+"TOPIC");
        sink.setStreamName1(moduleName);
        sink.setSinkName(moduleName);
        sink.setFunctionName1(moduleName);
        sink.setFunctionName2(moduleName);

        sinks.add(sink);
        map.put("sinks",sinks);

        File dir = new File("src/main/java/cn/flydiy/scala/evaluate");
        if(!dir.exists()){
            dir.mkdirs();
        }
        OutputStream fos = new FileOutputStream( new File(dir, moduleName +"Function.scala")); //java文件的生成目录
        Writer out = new OutputStreamWriter(fos);
        temp.process(map, out);

        fos.flush();
        fos.close();

        System.out.println("gen code success!");
    }

    public void genkuanbiaoFunction(String tableName,String zhubiao,String fkmx,String mingxi
    ,String max,String caiwu,String mx) throws IOException, TemplateException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(test.class,"/scripts");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        Template temp = cfg.getTemplate("KuanBiao_Function.ftl");
        String moduleName = NameConvert.entityName(tableName);
        Map<String, Object> map = new HashMap<>();
        map.put("packageName", "cn.flydiy.scala.evaluate");
        map.put("className", moduleName+"Function");
        map.put("mingxi", mingxi.toLowerCase());
        map.put("mingxiModule", mingxi);
        map.put("caiwu", caiwu.toLowerCase());
        map.put("caiwuModule", caiwu);
        map.put("moduleName", moduleName);
        map.put("maxModule", max);
        map.put("zhubiao", "azZhuBiao");
        map.put("zhubiaoIndex", "default_server_greeshinstall_tbl_az_assign_lc_ls_v2");
        map.put("esType", "_doc");
        map.put("esColumn", "pgguid");
        map.put("fxmx", "fkmxBiao");
        map.put("fxmxIndex", "default_server_greeshinstall_tbl_az_assign_fkmx_v2");
        map.put("esOrder", "cjdt");
        map.put("mx", "mxBiao");
        map.put("mxIndex", "default_server_greeshinstall_tbl_az_assign_mx_v2");

        List<String> modelFields = ReflectUtils.getModelFields(zhubiao);
        List<ColumnInfo> columns = new ArrayList<>();
        for (String field : modelFields) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnCode(field);
            columns.add(columnInfo);
        }
        map.put("zhubiaocolumns",columns);

        List<String> modelFields1 = ReflectUtils.getModelFields(fkmx);
        List<ColumnInfo> columns1 = new ArrayList<>();
        for (String field : modelFields1) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnCode(field);
            columns1.add(columnInfo);
        }
        map.put("fxmxcolumns",columns1);

        List<String> modelFields2 = ReflectUtils.getModelFields(mingxi);
        List<ColumnInfo> columns2 = new ArrayList<>();
        for (String field : modelFields2) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnCode(field);
            columns2.add(columnInfo);
        }
        map.put("mingxicolumns",columns2);

        List<String> modelFields3 = ReflectUtils.getModelFields(max);
        List<ColumnInfo> columns3 = new ArrayList<>();
        for (String field : modelFields3) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnCode(field);
            columns3.add(columnInfo);
        }
        map.put("maxcolumns",columns3);

        List<String> modelFields4 = ReflectUtils.getModelFields(caiwu);
        List<ColumnInfo> columns4 = new ArrayList<>();
        for (String field : modelFields4) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnCode(field);
            columns4.add(columnInfo);
        }
        map.put("caiwucolumns",columns4);

        List<String> modelFields5 = ReflectUtils.getModelFields(mx);
        List<ColumnInfo> columns5 = new ArrayList<>();
        for (String field : modelFields5) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnCode(field);
            columns5.add(columnInfo);
        }
        map.put("mxcolumns",columns5);

        File dir = new File("src/main/java/cn/flydiy/scala/evaluate");
        if(!dir.exists()){
            dir.mkdirs();
        }
        OutputStream fos = new FileOutputStream( new File(dir, moduleName +"Function.scala")); //java文件的生成目录
        Writer out = new OutputStreamWriter(fos);
        temp.process(map, out);

        fos.flush();
        fos.close();

        System.out.println("gen code success!");
    }

    public void genSingleTableFunction(String tableName) throws IOException, TemplateException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(test.class,"/scripts");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        Template temp = cfg.getTemplate("AzAssign_Function.ftl");
        String moduleName = NameConvert.entityName(tableName);
        Map<String, Object> map = new HashMap<>();
        map.put("packageName", "cn.flydiy.scala.evaluate");
        map.put("className", moduleName+"Function");
        map.put("stream", moduleName+"Stream");
        map.put("tableName", moduleName);

        List<String> modelFields = ReflectUtils.getModelFields(tableName);
        List<ColumnInfo> columns = new ArrayList<>();
        for (String field : modelFields) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnCode(field);
            columns.add(columnInfo);
        }
        map.put("columns",columns);

        File dir = new File("src/main/java/cn/flydiy/scala/evaluate");
        if(!dir.exists()){
            dir.mkdirs();
        }
        OutputStream fos = new FileOutputStream( new File(dir, moduleName +"Function.scala")); //java文件的生成目录
        Writer out = new OutputStreamWriter(fos);
        temp.process(map, out);

        fos.flush();
        fos.close();

        System.out.println("gen code success!");
    }

    public void genScalaModule(MysqlTable mysqlTable) throws IOException, TemplateException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(test.class,"/scripts");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        Template temp = cfg.getTemplate("Scala_Entity.ftl");

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
        model.setPackageName("cn.flydiy.scala.module");
        System.out.println("scalaModel::"+model);

        File dir = new File("src/main/java/cn/flydiy/scala/module");
        if(!dir.exists()){
            dir.mkdirs();
        }
        OutputStream fos = new FileOutputStream( new File(dir, model.getClassName()+".scala")); //java文件的生成目录
        Writer out = new OutputStreamWriter(fos);
        temp.process(model, out);

        fos.flush();
        fos.close();

        System.out.println("gen code success!");
    }

    public void genJavaModule(MysqlTable mysqlTable) throws IOException, TemplateException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(test.class,"/scripts");
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
        System.out.println("javaModel::"+model);

        File dir = new File("src\\main\\java");
        if(!dir.exists()){
            dir.mkdirs();
        }
        OutputStream fos = new FileOutputStream( new File(dir, model.getClassName()+".java")); //java文件的生成目录
        Writer out = new OutputStreamWriter(fos);
        temp.process(model, out);

        fos.flush();
        fos.close();

        System.out.println("gen code success!");
    }
}
