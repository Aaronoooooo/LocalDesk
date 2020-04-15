package cn.itcast.service;

import cn.itcast.domain.Product;

import java.util.List;

public interface ProductService {


    void save(List<Product> productList);


    List<Product> findAll();

}
