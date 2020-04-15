package com.flinksqlscala;

public class Student1 {
    private String studentId;
    private String studentName;
    private Integer studentAge;

    public Student1() {
    }

    public Student1(String studentId, String studentName, Integer studentAge) {
        this.studentId = studentId;
        this.studentName = studentName;
        this.studentAge = studentAge;
    }

    public String getStudentId() {
        return studentId;
    }

    public void setStudentId(String studentId) {
        this.studentId = studentId;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public Integer getStudentAge() {
        return studentAge;
    }

    public void setStudentAge(Integer studentAge) {
        this.studentAge = studentAge;
    }

    @Override
    public String toString() {
        return "Student1{" +
                "studentId='" + studentId + '\'' +
                ", studentName='" + studentName + '\'' +
                ", studentAge=" + studentAge +
                '}';
    }
}
