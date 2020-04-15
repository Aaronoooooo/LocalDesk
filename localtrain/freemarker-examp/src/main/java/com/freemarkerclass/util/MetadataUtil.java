package com.freemarkerclass.util;

/**
 * @author pengfeisu
 * @create 2020-02-12 13:19
 */


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 负责连接数据库、获取表的元信息、列的元信息
 */
public class MetadataUtil {
    private static Connection conn;//连接对象

    /*2. DatabaseMetaData接口常用的方法：获取数据库元数据
(1) ResultSet getTables(String catalog,String schemaPattern,String tableNamePattern,String[] types);   //获取表信息
(2) ResultSet getPrimaryKeys(String catalog,String schema,String table);  //获取表主键信息
(3) ResultSet getIndexInfo(String catalog,String schema,String table,boolean unique,boolean approximate);  //获取表索引信息
(4) ResultSet getColumns(String catalog,String schemaPattern,String tableNamePattern,String columnNamePattern); //获取表列信息*/
    private static DatabaseMetaData meta;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("数据库连接失败！");
        }
    }

    /**
     * 连接数据库获取数据库元数据
     */
    public static void openConnection() {
        try {
            if (conn == null || conn.isClosed()) {//isClosed:判断连接是否关闭
                conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/XXX?useUnicode=true&characterEncoding=utf-8"
                        , "XXX", "XXX");//连接数据库 XXX表示你自己的数据库名称 、用户名、密码
                meta = conn.getMetaData();//获取元数据
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取表的注释
     * @param
     * @return
     */
    public static String getCommentByTableName(String tableName) throws SQLException {
        openConnection();//打开连接
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName);
        String comment = null;
        if (rs != null && rs.next()) {
            comment=rs.getString(2);
            //判断字符，只获取注解部分
            int index = comment.lastIndexOf("=");
            comment = comment.substring(index+1);
        }
        rs.close();
        stmt.close();
        conn.close();
        return comment;
    }

    /**
     * 获取所有的表名
     * @return
     */
    public static String[] getTableNames(){
        openConnection();
        ResultSet rs=null;
        List<String> nameList = new ArrayList<>();
        try{
            rs=meta.getTables(null,
                    null,
                    null,
                    new String[]{"TABLE"});
            while (rs.next()){
                String tName =rs.getString("TABLE_NAME");
                nameList.add(tName);//将取出来的表名放入集合中
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return (String[])nameList.toArray(new String[]{});
    }

    /**
     * 获取某个表中所有的列信息
     * @param tableName
     * @return
     * @throws Exception
     */
    public static List<String[]> getTableColumnsInfo(String tableName)throws Exception{
        openConnection();
        ResultSet rs= meta.getColumns(null,
                "%",tableName,"%");
        List<String[]> columnInfoList =new ArrayList<>();
        while (rs.next()){
            String[] colInfo = new String[3];
            colInfo[0]=rs.getString("COLUMN_NAME");//COLUMN_NAME 列名
            colInfo[1]=rs.getString("REMARKS");//REMARKS 获取列注释
            colInfo[2]=rs.getString("TYPE_NAME");//TYPE_NAME 列类型
            columnInfoList.add(colInfo);
        }
        return columnInfoList;
    }
}