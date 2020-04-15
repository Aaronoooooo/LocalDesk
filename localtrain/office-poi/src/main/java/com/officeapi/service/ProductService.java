package com.officeapi.service;



import com.officeapi.domain.Product;

import java.util.List;

public interface ProductService {


    void save(List<Product> productList);


    List<Product> findAll();

}
