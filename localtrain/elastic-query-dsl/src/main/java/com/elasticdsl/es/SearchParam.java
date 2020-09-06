package com.elasticdsl.es;

import java.io.Serializable;

public class SearchParam<C> implements Serializable {
    private String id;
    private String name;
    private String index = "mongo_message2020";
    private String sql;
    private String date;
    private Integer page = 0;
    private int size = 20;
    private C criteria;

    public SearchParam() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public C getCriteria() {
        return this.criteria;
    }

    public void setCriteria(C criteria) {
        this.criteria = criteria;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
