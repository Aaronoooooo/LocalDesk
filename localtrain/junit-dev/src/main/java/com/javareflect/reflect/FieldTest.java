package com.javareflect.reflect;

import com.javareflect.interfaces.Person;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * 获取当前运行时类的属性结构
 */
public class FieldTest {

    @Test
    public void test1() {

        Class clazz = Person.class;

        //获取属性结构
        //getFields():获取当前运行时类及其父类中声明为public访问权限的属性
        Field[] fields = clazz.getFields();
        for (Field f : fields) {
            System.out.println(f);
        }
        System.out.println();

        //getDeclaredFields():获取当前运行时类中声明的所有属性。（不包含父类中声明的属性）
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field f : declaredFields) {
            System.out.println(f);
            System.out.println("获取字段类型: " + f.getType());
            System.out.println("获取字段类型toString: " + f.getType().toString());
            System.out.println("获取字段类型Generic: " + f.getType().toGenericString());
            System.out.println("获取字段名称: " + f.getName());
        }
    }

    //权限修饰符  数据类型 变量名
    @Test
    public void test2() {
        Class clazz = Person.class;
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field f : declaredFields) {
            //1.权限修饰符
            int modifier = f.getModifiers();
            System.out.println("权限修饰符" + Modifier.toString(modifier) + "\t");

            //2.数据类型
            String typeName = f.getType().getName();
            System.out.println("数据类型" + typeName + "\t");
            String typeNameTmp = typeName.replaceAll("java.lang.", "");
            System.out.println("replaceAll:" + typeNameTmp);

            //3.变量名
            String fName = f.getName();
            System.out.println("变量名" + fName);

            System.out.println();
        }

    }

}
