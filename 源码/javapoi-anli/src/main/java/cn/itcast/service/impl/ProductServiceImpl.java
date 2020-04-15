package cn.itcast.service.impl;

import cn.itcast.dao.ProductDao;
import cn.itcast.dao.impl.ProductDaoImpl;
import cn.itcast.domain.Product;
import cn.itcast.service.ProductService;

import java.util.List;

public class ProductServiceImpl implements ProductService {
    private  ProductDao productDao=new ProductDaoImpl();

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
