package com.javareflect.reflect;

import java.lang.reflect.Method;
import java.util.Set;

public class ClassUtilsTest {
	public static void main(String[] args) {
		String packageName = "java.lang"; //填入完整包名，如com.org.String
		//packageName包名     false：不遍历子包
		Set<String> classNames = ClassUtils.getClassName(packageName, true);
		if (classNames != null) {
			for (String className : classNames) {
				Class<?> aClass = null;
				try {
					aClass = Class.forName(className);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				// Field[] declaredFields = aClass.getDeclaredFields();
				Method[] methods = aClass.getMethods();
				for (Method m : methods) {
					// String name = m.getName();
					// Class<?> type = m.getType();
					System.out.println(className + "类中公共方法: " + m);
				}
				System.out.println(className);
			}
		}
	}
}
