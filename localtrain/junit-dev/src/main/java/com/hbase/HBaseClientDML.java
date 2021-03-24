package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


public class HBaseClientDML {

    private Connection conn = null;

    @Before
    public void getConn() throws Exception {
        // 构建一个连接对象
        // 会自动加载hbase-site.xml配置
        Configuration entries = HBaseConfiguration.create();
        // zookeeper地址
        entries.set("hbase.zookeeper.quorum", "bigdata1,bigdata2,bigdata3");
        // zookeeper 的端口应该大家都知道
        entries.set("hbase.zookeeper.property.clientPort", "2181");
        // /hbase-unsecure 是你们zookeeper 的hbase存储信息的znode
        //entries.set("zookeeper.znode.parent", "/hbase");
        // 这里由于 ambari 的端口不一样所以和原生的端口不一样这个 要注意
        //entries.set("hbase.master", "node124:16000");
        // 设置客户端连接用户名 - 无效，使用的是tanggaomeng用户
        //entries.set("hbase.client.username", "hbase");
        //entries.set("hbase.client.password", "");

        conn = ConnectionFactory.createConnection(entries);
    }

    /**
     * DML
     * 增
     * put
     */
    @Test
    public void testPut() throws Exception {

        // 获取一个操作指定表的对象，进行DML操作
        Table table = conn.getTable(TableName.valueOf("user_info"));

        // 构造要插入的数据为一个Put类型（一个put对象只能对应一个rowkey）的对象
        Put put = new Put(Bytes.toBytes("001"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("Beijing"));

        Put put2 = new Put(Bytes.toBytes("002"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("lisi"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("29"));
        put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("Shanghai"));

        ArrayList<Put> puts = new ArrayList<>();

        puts.add(put);
        puts.add(put2);

        // 执行插入操作
        table.put(puts);

        System.out.println("插入HBase表完成！");
        table.close();
        conn.close();

    }


    /**
     * DML
     * 循环大量插入
     */
    @Test
    public void testManyPuts() throws Exception {

        Table table = conn.getTable(TableName.valueOf("user_info"));
        ArrayList<Put> puts = new ArrayList<>();

        for (int i = 0; i <= 100; i++) {
            Put put = new Put(Bytes.toBytes("" + i));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang" + i));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes((18 + i) + ""));
            put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("Beijing" + i));

            puts.add(put);

        }

        table.put(puts);

        System.out.println("批量插入HBase表完成！");
        table.close();
        conn.close();
    }

    /**
     * DML
     * 删除
     */
    @Test
    public void testDelete() throws Exception {

        Table table = conn.getTable(TableName.valueOf("user_info"));

        // 构造一个对象封装要删除的数据信息
        Delete delete1 = new Delete(Bytes.toBytes("0"));
        Delete delete2 = new Delete(Bytes.toBytes("1"));
        Delete delete3 = new Delete(Bytes.toBytes("10"));
        delete3.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"));

        ArrayList<Delete> dels = new ArrayList<>();
        dels.add(delete1);
        dels.add(delete2);
        dels.add(delete3);

        table.delete(dels);

        System.out.println("删除HBase数据完成！");
        table.close();
        conn.close();
    }


    /**
     * DML
     * 查询
     */
    @Test
    public void testGet() throws Exception {

        Table table = conn.getTable(TableName.valueOf("user_info"));

        Get get = new Get(Bytes.toBytes("100"));

        Result result = table.get(get);

        // 从结果中取出用户指定的某个key的value
        byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("username"));
        System.out.println(new String(value));

        System.out.println("-------------------------------------");

        // 遍历整行结果中的所有kv单元格
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();

            byte[] rowArray = cell.getRowArray(); // 本kv所属的行键的字节数组
            byte[] familyArray = cell.getFamilyArray(); // 列族名的字节数组
            byte[] qualifierArray = cell.getQualifierArray(); // 列名的字节数组
            byte[] valueArray = cell.getValueArray(); // 值的字节数组

            System.out.println("行键：" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
            System.out.println("列族：" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
            System.out.println("列名：" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println("值：" + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));

        }
    }

    /**
     * DML
     * 按行键范围查询
     */

    @Test
    public void testScan() throws Exception {

        Table table = conn.getTable(TableName.valueOf("Student"));

        // 包含起始键，不包含结束键 [10, 100)
        // 但是如果真的想查询出末尾的那个行键，那么，可以在末尾行键上拼接一个不可见的字节（\000）
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(cell.getQualifierArray()));
            }
        }
    }

    @Test
    public void testScan1() throws Exception {

        Table table = conn.getTable(TableName.valueOf("Student"));

        // 包含起始键，不包含结束键 [10, 100)
        // 但是如果真的想查询出末尾的那个行键，那么，可以在末尾行键上拼接一个不可见的字节（\000）
        //Scan scan = new Scan(Bytes.toBytes("10"), Bytes.toBytes("11\001"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()) {
            Result result = iterator.next();
            // 遍历整行结果中的所有kv单元格
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();


                byte[] rowArray = cell.getRowArray();  //本kv所属的行键的字节数组
                byte[] familyArray = cell.getFamilyArray();  //列族名的字节数组
                byte[] qualifierArray = cell.getQualifierArray();  //列名的字节数据
                byte[] valueArray = cell.getValueArray(); // value的字节数组

                System.out.println("行键: " + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
                System.out.println("列族名: " + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.println("列名: " + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println("value: " + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("----------------------");
        }
    }

    @Test
    public void testScan2() throws Exception {

        Table table = conn.getTable(TableName.valueOf("Student"));
        //Configuration conf = HBaseConfiguration.create();
        //HTable table = new HTable(conf,"customer");

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("order"),Bytes.toBytes("number"));
        Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new
                SubstringComparator("Fli"));
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE,filter1);
        scan.setFilter(list);
        ResultScanner result = table.getScanner(scan);
        for(Result res:result){
            byte[] val = res.getValue(Bytes.toBytes("order"), Bytes.toBytes("number"));
            System.out.println("Row-value : "+Bytes.toString(val));
            System.out.println(res);
        }
        table.close();

        // 包含起始键，不包含结束键 [10, 100)
        // 但是如果真的想查询出末尾的那个行键，那么，可以在末尾行键上拼接一个不可见的字节（\000）
        //Scan scan = new Scan(Bytes.toBytes("10"), Bytes.toBytes("11\001"));
        //Scan scan = new Scan();
        //ResultScanner scanner = table.getScanner(scan);
        //
        //Iterator<Result> iterator = scanner.iterator();
        //
        //while (iterator.hasNext()) {
        //    Result result = iterator.next();
        //    // 遍历整行结果中的所有kv单元格
        //    CellScanner cellScanner = result.cellScanner();
        //    while (cellScanner.advance()) {
        //        Cell cell = cellScanner.current();
        //
        //
        //        byte[] rowArray = cell.getRowArray();  //本kv所属的行键的字节数组
        //        byte[] familyArray = cell.getFamilyArray();  //列族名的字节数组
        //        byte[] qualifierArray = cell.getQualifierArray();  //列名的字节数据
        //        byte[] valueArray = cell.getValueArray(); // value的字节数组
        //
        //        System.out.println("行键: " + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
        //        System.out.println("列族名: " + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
        //        System.out.println("列名: " + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
        //        System.out.println("value: " + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
        //    }
            System.out.println("----------------------");
        //}
    }

    @Test
    public void filterLimitValueRange() throws IOException {
        // A filter that matches cells whose values are between the given values
        Table table = conn.getTable(TableName.valueOf("Student"));
        ValueFilter valueGreaterFilter =
                new ValueFilter(
                        CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(Bytes.toBytes("Aaron")));
        //ValueFilter valueLesserFilter =
        //        new ValueFilter(
        //                CompareFilter.CompareOp.LESS_OR_EQUAL,
        //                new BinaryComparator(Bytes.toBytes("laowang")));

        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filter.addFilter(valueGreaterFilter);
        //filter.addFilter(valueLesserFilter);

        Scan scan = new Scan().setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("contact"));
            System.out.println(Bytes.toString(value));
            System.out.println(value);
        }
        //readWithFilter(projectId, instanceId, tableId, scan);
    }

    public class Filter_RowValue {
        //public static void main(String args[])throws IOException {
        //    Configuration conf = HBaseConfiguration.create();
        //        //    HTable table = new HTable(conf,"customer");
        //    Scan scan = new Scan();
        //    scan.addColumn(Bytes.toBytes("order"),Bytes.toBytes("number"));
        //    Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new
        //            SubstringComparator("Fli"));
        //    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE,filter1);
        //    scan.setFilter(list);
        //    ResultScanner result = table.getScanner(scan);
        //    for(Result res:result){
        //        byte[] val = res.getValue(Bytes.toBytes("order"), Bytes.toBytes("number"));
        //        System.out.println("Row-value : "+Bytes.toString(val));
        //        System.out.println(res);
        //    }
        //    table.close();
        //}
    }

}
