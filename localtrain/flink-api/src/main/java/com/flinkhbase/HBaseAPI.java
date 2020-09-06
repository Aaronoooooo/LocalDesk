package com.flinkhbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


public class HBaseAPI {
    private static final Logger log = LoggerFactory.getLogger(HBaseAPI.class);

    private static final String TABLE_NAME = "user";
    private static final String TABLE_DESCRIPTOR = "Student";
    private static final String COLUMN_FAMILY_BASE = "base";
    private static final String COLUMN_FAMILY_ADDRESS = "address";
    private static final String COLUMN_USERNAME = "username";
    private static final String COLUMN_PASSWORD = "password";
    private static final String COLUMN_HOME = "home";
    private static final String COLUMN_OFFICE = "office";

    private Connection connection;

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "node124,node125,node126");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        //config.set("hbase.master", "node124:16000");
        Connection connection = ConnectionFactory.createConnection(config);

//        long t1 = System.currentTimeMillis();
        HBaseAPI t = new HBaseAPI(connection);
//        HBaseAPI t = new HBaseAPI();
//        t.createTableTest();
//        t.deleteTable();
        t.listTable();
//        t.createTable();
//        t.putRows();
//        t.listTable();
//        t.getRows();
//        t.deleteRows();
//        t.getRows();
//        t.deleteTable();
//        long t2 = System.currentTimeMillis();
//        System.out.println("Time: " + (t2 - t1));

//        connection.close();
    }

    public HBaseAPI() {
    }

    public HBaseAPI(Connection connection) {
        this.connection = connection;
    }

    private void listTable() throws IOException {
        Scan scan = new Scan();
        Admin admin = connection.getAdmin();
        //try {
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for (TableDescriptor tableDescriptor : tableDescriptors) {
//                KYLIN_SJI4AB8VQ6
//                KYLIN_TOQUYV4R7J
//                KYLIN_YINV1ZETW2
//                kylin_metadata
                TableName tableName = tableDescriptor.getTableName();
                System.out.println("Table:" + tableName + ", TableEnabled:" /*+ admin.isTableEnabled(tableName)*/);
            }
//            Table tables = connection.getTable(TableName.valueOf(TABLE_NAME));
//            ResultScanner resultScanner = tables.getScanner(scan);
//            for (Result result : resultScanner) {
//                Cell[] cells = result.rawCells();
//                for (Cell cell : cells) {
//                    //得到 rowkey
////                        System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
//                    //得到列族
//                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
//                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
//                    System.out.println(">>表Descriptor:" + tables.getName() + " 列族: " + family + " 列:" + column);
//                        System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
//                }
//            }
//        } finally {
            admin.close();
        //}
    }

//"{
//    "customHbase":
//            [
//    {"hbaseTable":"sys_org","hbaseFamily":"abbr","hbaseColumn":[{"province"},{"city"}]},{},{}
//            ],
//    "host": "flydiysz.cn",
//    "port": "32182"
//    }"


    private void createTable() throws IOException {
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        try {
            if (!admin.tableExists(table.getName())) {
                TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY_BASE)).build())
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY_ADDRESS)).build())
                        .build();
                admin.createTable(tableDesc);
                System.out.println("表" + TABLE_NAME + "创建成功！");
            }
        } finally {
            admin.close();
        }
    }

    private void deleteTable() throws IOException {
        Admin admin = connection.getAdmin();
        try {
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for (TableDescriptor tableDescriptor : tableDescriptors) {
                TableName deleteTableName = tableDescriptor.getTableName();
                if (deleteTableName.toString().equals(TABLE_NAME)) {
                    admin.disableTable(deleteTableName);
                    admin.deleteTable(deleteTableName);
                    System.out.println("表" + deleteTableName + "删除成功！");
                }
else {
                    System.out.println("表" + deleteTableName + "不存在！");
                }

//                System.out.println("Table: " + tableName);
//                System.out.println("\texists: " + admin.tableExists(tableName));
//                System.out.println("\tenabled: " + admin.isTableEnabled(tableName));
            }
        } finally {
            admin.close();
        }
    }

    //批量插入
    private void putRows() throws IOException {
        for (int i = 0; i < 10; i++) {
            putRow("row_" + i, "user_" + i, "password_" + i, "home_" + i, "office_" + i);
        }
        System.out.println(TABLE_NAME + "插入数据成功");
    }

    private void putRow(String row, String username, String password, String home, String office) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASE), Bytes.toBytes(COLUMN_USERNAME), Bytes.toBytes(username));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASE), Bytes.toBytes(COLUMN_PASSWORD), Bytes.toBytes(password));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_ADDRESS), Bytes.toBytes(COLUMN_HOME), Bytes.toBytes(home));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_ADDRESS), Bytes.toBytes(COLUMN_OFFICE), Bytes.toBytes(office));
        table.put(put);
        table.close();
    }

    private void getRows() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        System.out.println("getRows:" + resultScanner.iterator().next());
        Iterator<Result> it = resultScanner.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            getRow(result);
        }
        table.close();
    }

    private void getRow(String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        getRow(result);
        table.close();
    }

    private void getRow(Result result) {
        if (Bytes.toString(result.getRow()) != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(Bytes.toString(result.getRow()));
            sb.append("[");
            sb.append("base:username=" + Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("username"))));
            sb.append(", base:password=" + Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("password"))));
            sb.append(", address:home=" + Bytes.toString(result.getValue(Bytes.toBytes("address"), Bytes.toBytes("home"))));
            sb.append(", address:office=" + Bytes.toString(result.getValue(Bytes.toBytes("address"), Bytes.toBytes("office"))));
            sb.append("]");
            System.out.println(sb.toString());
        }
    }

    private void deleteRows() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);

        Iterator<Result> it = resultScanner.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            Delete delete = new Delete(result.getRow());
            table.delete(delete);
        }
        System.out.println("deleteRows...ing");
        table.close();
    }

    private void deleteRow(String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
        table.close();
    }

//bulk operation

//    @Test
    public JSONArray createTableTest() {
        String a = "{\"customHbase\":[{\"hbaseTable\":\"creHbase5\",\"hbaseFamily\":\"familyTest1\"},{\"hbaseTable\":\"creHbase6\",\"hbaseFamily\":\"familyTest2\"}],\"host\": \"cdh101\",\"port\": \"32181\"}";
        JSONObject jsonObject = JSON.parseObject(a);
        String host = jsonObject.getString("host");
        String port = jsonObject.getString("port");
        JSONArray customHBasesArr = null;
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", host);
            config.set("hbase.zookeeper.property.clientPort", port);
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();

            customHBasesArr = jsonObject.getJSONArray("customHbase");

            for (int i = 0; i < customHBasesArr.size(); i++) {
                JSONObject hbaseJSONObject = customHBasesArr.getJSONObject(i);
                String hbaseTable = hbaseJSONObject.getString("hbaseTable");
                String hbaseFamily = hbaseJSONObject.getString("hbaseFamily");
                Table table = connection.getTable(TableName.valueOf(hbaseTable));

                if (!admin.tableExists(table.getName())) {
                    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(hbaseTable))
                            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(hbaseFamily)).build())
                            .build();
                    admin.createTable(tableDesc);
                    System.out.println("表" + hbaseTable + "创建成功！");
                    log.info("表" + hbaseTable + "创建成功！");
                }
            }
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for (TableDescriptor tableDescriptor : tableDescriptors) {
                TableName tableName = tableDescriptor.getTableName();
                System.out.println("所有Table:" + tableName);
                log.info("所有Table:" + tableName);
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("customHBasesArrJSONArray: " + customHBasesArr);
        log.info("customHBasesArrJSONArray: " + customHBasesArr);
        return customHBasesArr;
    }

//批量插入数据

    @Test
    public void createTableTest2() {
        String aa = "{\"rows\":[{\"row\":\"1001\"},{\"row\":\"1002\"}],\"columns\":[{\"column\":\"user\"},{\"column\":\"addr\"}],\"customHbase\":[{\"hbaseTable\":\"creHbase3\",\"hbaseFamily\":\"familyTest1\"},{\"hbaseTable\":\"creHbase4\",\"hbaseFamily\":\"familyTest2\"}],\"values\":[{\"value\":\"zhangsan\"},{\"value\":\"lisi\"}],\"host\":\"cdh101\",\"port\":\"32181\"}";
        JSONObject jsonObject = JSON.parseObject(aa);
        String host = jsonObject.getString("host");
        String port = jsonObject.getString("port");

        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", host);
            config.set("hbase.zookeeper.property.clientPort", port);
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();


            JSONArray customHbase = jsonObject.getJSONArray("customHbase");
            JSONArray rows = jsonObject.getJSONArray("rows");
            JSONArray columns = jsonObject.getJSONArray("columns");
            JSONArray values = jsonObject.getJSONArray("values");

            for (int i = 0; i < customHbase.size(); i++) {
                JSONObject hbaseJSONObject = customHbase.getJSONObject(i);
                String hbaseTable = hbaseJSONObject.getString("hbaseTable");
                String hbaseFamily = hbaseJSONObject.getString("hbaseFamily");
                Table table = connection.getTable(TableName.valueOf(hbaseTable));

                if (!admin.tableExists(table.getName())) {
                    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(hbaseTable))
                            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(hbaseFamily)).build())
                            .build();
                    admin.createTable(tableDesc);
                    System.out.println("表" + hbaseTable + "创建成功！");

                    for (int j = 0; j < rows.size(); j++) {
                        JSONObject objectRow = rows.getJSONObject(j);
                        String row = objectRow.getString("row");
                        Put put = new Put(Bytes.toBytes(row));
                        for (int k = 0; k < columns.size(); k++) {
                            JSONObject objectColumn = columns.getJSONObject(k);
                            String column = objectColumn.getString("column");
                            for (int ii = 0; ii < values.size(); ii++) {
                                JSONObject objectValue = values.getJSONObject(ii);
                                String value = objectValue.getString("value");
                                put.addColumn(Bytes.toBytes(hbaseFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                                table.put(put);
                                log.info(hbaseTable + "插入数据成功");
                            }
                        }
                    }
                }
//                table.close();
            }
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for (TableDescriptor tableDescriptor : tableDescriptors) {
                TableName tableName = tableDescriptor.getTableName();
                System.out.println("所有Table:" + tableName);
            }
//            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
