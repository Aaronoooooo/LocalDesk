package com.javalistmap;

/**
 * @author pengfeisu
 * @create 2020-02-25 19:17
 */
public class Student {
    public String age = "age";
    public String name = "name";

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Student(String age, String name) {
        this.age = age;
        this.name = name;
    }

    public Student() {
    }

    @Override
    public String toString() {
        return "Student{" +
                "age='" + age + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
