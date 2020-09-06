package com.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

public class HBaseClientDDL {
    private Connection conn = null;

    @Before
    public void getConn() throws Exception {
        // 构建一个连接对象
        // 会自动加载hbase-site.xml配置
        Configuration entries = HBaseConfiguration.create();
        // zookeeper地址
        entries.set("hbase.zookeeper.quorum", "104.250.140.226,104.250.133.2,104.250.129.202");
        // zookeeper 的端口应该大家都知道
        entries.set("hbase.zookeeper.property.clientPort", "2181");
        // /hbase-unsecure 是你们zookeeper 的hbase存储信息的znode
        entries.set("zookeeper.znode.parent", "/hbase");
        // 这里由于 ambari 的端口不一样所以和原生的端口不一样这个 要注意
        entries.set("hbase.master", "104.250.129.202:16000");
        // 设置客户端连接用户名 - 无效，使用的是win10终端tanggaomeng用户
        //entries.set("hbase.client.username", "hbase");
        //entries.set("hbase.client.password", "");

        conn = ConnectionFactory.createConnection(entries);
    }

    /**
     * DDL
     * 创建表
     */
    @Test
    public void testCreateTable() throws Exception {

        //从链接中构造一个DDL操作器
        Admin admin = conn.getAdmin();

        //创建一个表定义描述对象
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));

        //创建列族定义描述对象
        HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");
        //设置该列族中存储数据的最大版本数，默认是1
        hColumnDescriptor_1.setMaxVersions(2);

        HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("extra_info");

        //将列族定义信息对象放入表定义对象中
        hTableDescriptor.addFamily(hColumnDescriptor_1);
        hTableDescriptor.addFamily(hColumnDescriptor_2);

        //使用ddl操作器对象：admin 来创建表
        admin.createTable(hTableDescriptor);

        System.out.println("创建HBase表完成！");
        //关闭连接
        admin.close();
        conn.close();

    }

    /**
     * DDL
     * 修改表
     * 添加列族
     */
    @Test
    public void testAlterTable() throws Exception {

        Admin admin = conn.getAdmin();

        // 取出旧表的定义信息
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));

        // 新构造一个列族定义
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
        // 设置该列族的布隆过滤器类型
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);

        // 将列族定义添加到列表定义对象中
        tableDescriptor.addFamily(hColumnDescriptor);

        // 将修改过的表定义交给admin去提交
        admin.modifyTable(TableName.valueOf("user_info"), tableDescriptor);

        System.out.println("修改HBase表完成！");
        admin.close();
        conn.close();
    }

    /**
     * DDL
     * 删除表
     */
    @Test
    public void testDropTable() throws Exception {

        Admin admin = conn.getAdmin();

        // 停用表
        admin.disableTable(TableName.valueOf("user_info"));
        // 删除表
        admin.deleteTable(TableName.valueOf("user_info"));

        System.out.println("删除HBase表完成！");
        admin.close();
        conn.close();

    }


}