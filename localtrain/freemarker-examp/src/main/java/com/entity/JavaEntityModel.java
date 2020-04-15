package com.entity;

import java.util.*;

/**
 * 数据库映射
 */
public class JavaEntityModel {

    /**
     * java 中不需要引包的类型
     */
    private static List<String> baseClass = Arrays.asList(
            int.class.getName(),
            double.class.getName(),
            float.class.getName(),
            long.class.getName(),
            short.class.getName(),
            byte.class.getName(),
            char.class.getName(),
            boolean.class.getName(),
            String.class.getName()
    );

    /**
     * 类名
     */
    private String className;

    /**
     * 类 包名
     */
    private String packageName;

    /**
     * K:属性名称
     * V:属性类型
     */
    private Map<String, Class> fields = new HashMap<>();


    private List<String> imports = new ArrayList<>();

    /**
     * 添加需要导入的包
     *
     * @param className
     */
    public void addImport(String className) {
        if (baseClass.indexOf(className) != -1) {
            return;
        }
        if (imports.indexOf(className) == -1) {
            imports.add(className);
        }
    }

    /**
     * 添加属性
     *
     * @param fieldName  属性名称
     * @param fieldClass 属性类型
     */
    public void addfield(String fieldName, Class fieldClass) {
        if (!fields.containsKey(fieldName)) {
            fields.put(fieldName, fieldClass);
        }
    }

    public static List<String> getBaseClass() {
        return baseClass;
    }

    public static void setBaseClass(List<String> baseClass) {
        JavaEntityModel.baseClass = baseClass;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public Map<String, Class> getFields() {
        return fields;
    }

    public void setFields(Map<String, Class> fields) {
        this.fields = fields;
    }

    public List<String> getImports() {
        return imports;
    }

    public void setImports(List<String> imports) {
        this.imports = imports;
    }

    @Override
    public String toString() {
        return "JavaEntityModel{" +
                "className='" + className + '\'' +
                ", packageName='" + packageName + '\'' +
                ", fields=" + fields +
                ", imports=" + imports +
                '}';
    }
}
