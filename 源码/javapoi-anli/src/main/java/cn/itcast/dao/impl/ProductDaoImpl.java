package cn.itcast.dao.impl;

import cn.itcast.dao.ProductDao;
import cn.itcast.domain.Product;
import cn.itcast.utils.JDBCUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class ProductDaoImpl implements ProductDao {
        JdbcTemplate jdbcTemplate=new JdbcTemplate(JDBCUtils.getDataSource());

    @Override
    public void save(Product product) {
        String sql="insert into product values(?,?,?,?)";
        jdbcTemplate.update(sql,product.getPid(),product.getPname(),product.getPrice(),product.getPstock());

    }

    @Override
    public List<Product> findAll() {
        String sql="select * from product";
        return jdbcTemplate.query(sql,new BeanPropertyRowMapper<Product>(Product.class));
    }


}
