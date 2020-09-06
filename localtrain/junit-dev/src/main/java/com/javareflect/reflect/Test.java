package com.javareflect.reflect;

import java.lang.reflect.Field;
import java.util.*;

class A {
    private String id;
    private String name;
}

class B extends A {
    private String sex = "男";

    public String getSex() {
        return sex;
    }
}

public class Test {
    public static void main(String[] args) {
        B b = new B();
        A a = new A();
        List list = getFieldList(a.getClass());
        for (int i = 0; i < list.size(); i++) {
            Field field = (Field)list.get(i);
            Object o = list.get(i);
            Class type = field.getType();
            //String name = field.getName();
            Class<A> aClass = A.class;
            Class<? extends A> aClass1 = a.getClass();
            Field[] fields1 = aClass1.getDeclaredFields();
            Field[] fields = aClass.getDeclaredFields();
            String[] strings = new String[fields.length];
            for (int i1 = 0; i1 < fields.length; i1++) {
                strings[i] = fields[i].getName();
            }

            String[] strings2 = new String[fields1.length];
            for (int i1 = 0; i1 < fields1.length; i1++) {
                strings2[i] = fields1[i].getName();
            }
            System.out.println();


            boolean assignableFromA = A.class.isAssignableFrom(type);
            boolean assignableFromB1 = A.class.isAssignableFrom(b.getClass());
            boolean assignableFromB2 = String.class.isAssignableFrom(b.getSex().getClass());
            boolean assignableFromO = String.class.isAssignableFrom(type);
            boolean assignableFromI = Integer.class.isAssignableFrom(type);
            System.out.println();
        }

        //A ba = new B();
        System.out.println("1-------------");
        System.out.println(A.class.isAssignableFrom(a.getClass()));
        System.out.println(B.class.isAssignableFrom(b.getClass()));
        System.out.println(A.class.isAssignableFrom(b.getClass()));
        System.out.println(B.class.isAssignableFrom(a.getClass()));
        System.out.println("2-------------");
        System.out.println(a.getClass().isAssignableFrom(A.class));
        System.out.println(b.getClass().isAssignableFrom(B.class));
        System.out.println(a.getClass().isAssignableFrom(B.class));
        System.out.println(b.getClass().isAssignableFrom(A.class));
        System.out.println("3-------------");
        System.out.println(Object.class.isAssignableFrom(b.getClass()));
        System.out.println(Object.class.isAssignableFrom("abc".getClass()));
        System.out.println("4-------------");
        System.out.println("a".getClass().isAssignableFrom(Object.class));
        System.out.println("abc".getClass().isAssignableFrom(Object.class));
    }


    private static final Map<String, List<Field>> cacheFields = new HashMap();
    private static List<Field> getFieldList(Class tempClass) {
        String curBeanName = tempClass.getName();
        List<Field> curBeanFields = (List)cacheFields.get(curBeanName);
        if (curBeanFields != null) {
            return curBeanFields;
        } else {
            ArrayList fieldList;
            for(fieldList = new ArrayList(); tempClass != null; tempClass = tempClass.getSuperclass()) {
                List list = Arrays.asList(tempClass.getDeclaredFields());
                if (list.size() > 0) {
                    fieldList.addAll(list);
                }
            }

            cacheFields.put(curBeanName, fieldList);
            return fieldList;
        }
    }
}
