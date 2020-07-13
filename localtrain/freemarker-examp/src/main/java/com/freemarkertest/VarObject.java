package com.freemarkertest;

import java.util.List;

public class VarObject {

        private String varCreateName;
        private String varCreateType;
        private List<String> varCreateValue;
        private String varCreateOutName;
        private String varChangeName;
        private String varChangeType;
        private List<String> varChangeValue;
        private String varChangeOutName;
        private List<String> varIfMethod;
        private String varIfOutName;
        private List<String> varExeMethod;
        private String methResulValue;
        private String methResulType;
        private String methName;
        private String methNameInParamName;
        private String methNameInParamType;

    public VarObject() {
    }

    public VarObject(String varCreateName, String varCreateType, List<String> varCreateValue, String varCreateOutName, String varChangeName, String varChangeType, List<String> varChangeValue, String varChangeOutName, List<String> varIfMethod, String varIfOutName, List<String> varExeMethod, String methResulValue, String methResulType, String methName, String methNameInParamName, String methNameInParamType) {
        this.varCreateName = varCreateName;
        this.varCreateType = varCreateType;
        this.varCreateValue = varCreateValue;
        this.varCreateOutName = varCreateOutName;
        this.varChangeName = varChangeName;
        this.varChangeType = varChangeType;
        this.varChangeValue = varChangeValue;
        this.varChangeOutName = varChangeOutName;
        this.varIfOutName = varIfOutName;
        this.methResulValue = methResulValue;
        this.methResulType = methResulType;
        this.methName = methName;
        this.methNameInParamName = methNameInParamName;
        this.methNameInParamType = methNameInParamType;
    }

    public String getVarCreateName() {
        return varCreateName;
    }

    public void setVarCreateName(String varCreateName) {
        this.varCreateName = varCreateName;
    }

    public String getVarCreateType() {
        return varCreateType;
    }

    public void setVarCreateType(String varCreateType) {
        this.varCreateType = varCreateType;
    }

    public List<String> getVarCreateValue() {
        return varCreateValue;
    }

    public void setVarCreateValue(List<String> varCreateValue) {
        this.varCreateValue = varCreateValue;
    }

    public String getVarCreateOutName() {
        return varCreateOutName;
    }

    public void setVarCreateOutName(String varCreateOutName) {
        this.varCreateOutName = varCreateOutName;
    }

    public String getVarChangeName() {
        return varChangeName;
    }

    public void setVarChangeName(String varChangeName) {
        this.varChangeName = varChangeName;
    }

    public String getVarChangeType() {
        return varChangeType;
    }

    public void setVarChangeType(String varChangeType) {
        this.varChangeType = varChangeType;
    }

    public List<String> getVarChangeValue() {
        return varChangeValue;
    }

    public void setVarChangeValue(List<String> varChangeValue) {
        this.varChangeValue = varChangeValue;
    }

    public String getVarChangeOutName() {
        return varChangeOutName;
    }

    public void setVarChangeOutName(String varChangeOutName) {
        this.varChangeOutName = varChangeOutName;
    }

    public String getVarIfOutName() {
        return varIfOutName;
    }

    public void setVarIfOutName(String varIfOutName) {
        this.varIfOutName = varIfOutName;
    }

    public String getMethResulValue() {
        return methResulValue;
    }

    public void setMethResulValue(String methResulValue) {
        this.methResulValue = methResulValue;
    }

    public String getMethResulType() {
        return methResulType;
    }

    public void setMethResulType(String methResulType) {
        this.methResulType = methResulType;
    }

    public String getMethName() {
        return methName;
    }

    public void setMethName(String methName) {
        this.methName = methName;
    }

    public String getMethNameInParamName() {
        return methNameInParamName;
    }

    public void setMethNameInParamName(String methNameInParamName) {
        this.methNameInParamName = methNameInParamName;
    }

    public String getMethNameInParamType() {
        return methNameInParamType;
    }

    public void setMethNameInParamType(String methNameInParamType) {
        this.methNameInParamType = methNameInParamType;
    }

    public List<String> getVarIfMethod() {
        return varIfMethod;
    }

    public void setVarIfMethod(List<String> varIfMethod) {
        this.varIfMethod = varIfMethod;
    }

    public List<String> getVarExeMethod() {
        return varExeMethod;
    }

    public void setVarExeMethod(List<String> varExeMethod) {
        this.varExeMethod = varExeMethod;
    }

    @Override
    public String toString() {
        return "VarObject{" +
                "varCreateName='" + varCreateName + '\'' +
                ", varCreateType='" + varCreateType + '\'' +
                ", varCreateValue=" + varCreateValue +
                ", varCreateOutName='" + varCreateOutName + '\'' +
                ", varChangeName='" + varChangeName + '\'' +
                ", varChangeType='" + varChangeType + '\'' +
                ", varChangeValue=" + varChangeValue +
                ", varChangeOutName='" + varChangeOutName + '\'' +
                ", varIfMethod=" + varIfMethod +
                ", varIfOutName='" + varIfOutName + '\'' +
                ", varExeMethod=" + varExeMethod +
                ", methResulValue='" + methResulValue + '\'' +
                ", methResulType='" + methResulType + '\'' +
                ", methName='" + methName + '\'' +
                ", methNameInParamName='" + methNameInParamName + '\'' +
                ", methNameInParamType='" + methNameInParamType + '\'' +
                '}';
    }
}
