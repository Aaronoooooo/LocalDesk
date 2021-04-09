package com.springapi.domain;



public class Product {
    private Integer pid;
    private String pname;
    private double price;
    private int pstock;

    @Override
    public String toString() {
        return "Product{" +
                "pid=" + pid +
                ", pname='" + pname + '\'' +
                ", price=" + price +
                ", pstock=" + pstock +
                '}';
    }

    public Product(Integer pid, String pname, double price, int pstock) {
        this.pid = pid;
        this.pname = pname;
        this.price = price;
        this.pstock = pstock;
    }

    public Product() {
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getPstock() {
        return pstock;
    }

    public void setPstock(int pstock) {
        this.pstock = pstock;
    }
}