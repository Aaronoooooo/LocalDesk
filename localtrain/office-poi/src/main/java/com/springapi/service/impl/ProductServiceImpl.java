package com.springapi.service.impl;


import com.springapi.dao.ProductDao;
import com.springapi.dao.impl.ProductDaoImpl;
import com.springapi.domain.Product;
import com.springapi.service.ProductService;

import java.util.List;

public class ProductServiceImpl implements ProductService {
    private ProductDao productDao = new ProductDaoImpl();

    @Override
    public void save(List<Product> productList) {
        for (Product product : productList) {
            productDao.save(product);
        }

    }

    @Override
    public List<Product> findAll() {

        return productDao.findAll();
    }


}
