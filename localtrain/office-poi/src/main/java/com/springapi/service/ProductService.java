package com.springapi.service;



import com.springapi.domain.Product;

import java.util.List;

public interface ProductService {


    void save(List<Product> productList);


    List<Product> findAll();

}
