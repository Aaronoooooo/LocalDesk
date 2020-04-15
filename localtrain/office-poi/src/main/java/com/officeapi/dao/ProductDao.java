package com.officeapi.dao;



import com.officeapi.domain.Product;

import java.util.List;

public interface ProductDao {

    void save(Product product);


    List<Product> findAll();

}
