package com.freemarkerclass.generator;

/**
 * @author pengfeisu
 * @create 2020-02-12 13:45
 */

import com.freemarkerclass.vo.*;
import com.freemarkerclass.util.*;
import freemarker.template.Configuration;
import freemarker.template.Template;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 抽象生成器
 */
public abstract class AbstractGenerator {
    protected Configuration cfg;//Freemarker配置对象
    protected Map valueMap;//填充到模板的数据
    protected Template template;//模板对象
    protected String savePath;//保存生成文件的路径
    protected List<TableVo> tableList;//表信息

    public AbstractGenerator() throws Exception {
        cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(this.getClass(), "/templates/"); //获取模板文件位置
        valueMap = new HashMap();
        init();//初始化数据
    }

    /**
     * 获取所有的表信息列信息
     * @throws Exception
     */
    public void init()throws Exception{
        tableList=new ArrayList<>();
        String[] tableNameArr =MetadataUtil.getTableNames();//获取表名称
        for(String tName:tableNameArr){
            String tComment = MetadataUtil.getCommentByTableName(tName);//获取注释
            TableVo table = new TableVo();//创建表信息对象
            table.setClassName(JavaNameUtil.toPascal(tName));
            table.setComment(tComment);
            table.setTableName(tName);
            List<String[]> colInfoList =MetadataUtil.getTableColumnsInfo(tName);
            for (String[] colInfo:colInfoList){
                String cName =colInfo[0];
                String cComment =colInfo[1];
                String cType =colInfo[2];
                ColumnVo column = new ColumnVo();
                column.setComment(cComment);
                column.setName(cName);
                column.setFieldName(JavaNameUtil.toCamel(cName));
                column.setDbType(cType);
                column.setJavaType(JavaNameUtil.dbtype2JavaType(cType));
                column.setUpperCaseColumnName(JavaNameUtil.toPascal(cName));
                table.getColumns().add(column);
            }
            tableList.add(table);
            System.out.println(table);
        }
        System.out.println("构建元数据成功！");
    }

    //抽象生成代码文件，各个子类实现
    public abstract void generateCode() throws Exception;

    public void setSavePath(String savePath) {
        this.savePath = savePath;
    }
}
