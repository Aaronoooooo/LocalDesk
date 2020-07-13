package com.springapi.dao;



import com.springapi.domain.Product;

import java.util.List;

public interface ProductDao {

    void save(Product product);


    List<Product> findAll();

}
