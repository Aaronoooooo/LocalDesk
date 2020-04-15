package com.entity;

import java.util.List;

public class MysqlTable {
    private String tName;
    private List<MysqlColumn> colNames;

    @Override
    public String toString() {
        return "MysqlTable{" +
                "tName='" + tName + '\'' +
                ", colNames=" + colNames +
                '}';
    }

    public String gettName() {
        return tName;
    }

    public void settName(String tName) {
        this.tName = tName;
    }

    public List<MysqlColumn> getColNames() {
        return colNames;
    }

    public void setColNames(List<MysqlColumn> colNames) {
        this.colNames = colNames;
    }
}
