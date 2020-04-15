package cn.itcast.dao;

import cn.itcast.domain.Product;

import java.util.List;

public interface ProductDao {

    void save(Product product);


    List<Product> findAll();

}
