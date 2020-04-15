package com.freemarkerclass.vo;

/**
 * @author pengfeisu
 * @create 2020-02-12 13:19
 */


/**
 * 列对象
 */
public class ColumnVo {
    private String name;//数据库中的列名
    private String fieldName;//对应的Java属性名

    private String dbType;//数据中的记录类型
    private String javaType;//对应的Java属性类型

    private String comment;//数据库中的注释
    private String upperCaseColumnName;//转成帕斯卡后的属性名

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getJavaType() {
        return javaType;
    }

    public void setJavaType(String javaType) {
        this.javaType = javaType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getUpperCaseColumnName() {
        return upperCaseColumnName;
    }

    public void setUpperCaseColumnName(String upperCaseColumnName) {
        this.upperCaseColumnName = upperCaseColumnName;
    }
}
