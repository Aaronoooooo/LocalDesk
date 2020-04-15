package com.freemarkerclass.vo;

/**
 * @author pengfeisu
 * @create 2020-02-12 13:19
 */

import java.util.ArrayList;
import java.util.List;

/**
 * 表对象
 */
public class TableVo {
    private String className;//帕斯卡风格的类名
    private String tableName;//表名
    private String comment;//注释
    private String lowerClassName;//骆驼命名法


    private List<ColumnVo> columns=new ArrayList<ColumnVo>();//列对象集合

    public String getLowerClassName() {
        return lowerClassName;
    }

    public void setLowerClassName(String lowerClassName) {
        this.lowerClassName = lowerClassName;
    }



    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public List<ColumnVo> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnVo> columns) {
        this.columns = columns;
    }
}