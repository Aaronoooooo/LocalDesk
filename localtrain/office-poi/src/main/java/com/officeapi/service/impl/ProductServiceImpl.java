package com.officeapi.service.impl;


import com.officeapi.dao.ProductDao;
import com.officeapi.dao.impl.ProductDaoImpl;
import com.officeapi.domain.Product;
import com.officeapi.service.ProductService;

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
