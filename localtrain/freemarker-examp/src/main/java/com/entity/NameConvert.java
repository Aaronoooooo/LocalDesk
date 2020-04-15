package com.entity;

/**
 * 字段名称转换
 */
public class NameConvert {

    /**
     * 是否保留前缀
     */
    public static boolean keepPrefix = true;

    /**
     * 类名称转换
     *
     * @param tableName
     * @return
     */
    public static String entityName(String tableName) {
        String lowerCaseName = tableName.toLowerCase();
        StringBuilder newName = new StringBuilder();
        char[] chars = lowerCaseName.toCharArray();
        //是否是前缀
        boolean prefix = true;
        //是否转换大写
        boolean change = false;
        for (int i = 0; i < chars.length; i++) {
            char aChar = chars[i];
            if (aChar == '_' && !change) {
                change = true;
                //已经遇到_符号，表示后面的不是前缀
                if (prefix) {
                    prefix = false;
                }
                continue;
            }
            //首字母大写
            if (i == 0) {
                aChar = Character.toUpperCase(aChar);
            }
            if (change) {
                aChar = Character.toUpperCase(aChar);
                change = false;
            }
            if (keepPrefix) {
                newName.append(aChar);
                continue;
            }
            if (!prefix) {
                newName.append(aChar);
                continue;
            }
        }
        return newName.toString();
    }

    /**
     * 属性名称转换
     *
     * @param name
     * @return
     */
    public static String fieldName(String name) {
        name = name.toLowerCase();
        StringBuilder newName = new StringBuilder();
        //name_t_able按单个字符拆开
        char[] chars = name.toCharArray();
        boolean change = false;
        for (int i = 0; i < chars.length; i++) {
            char aChar = chars[i];
            if (aChar == '_' && !change) {
                change = true;
                continue;
            }
            if (change) {
                aChar = Character.toUpperCase(aChar);
                change = false;
            }
            newName.append(aChar);
        }
        return newName.toString();
    }
}
