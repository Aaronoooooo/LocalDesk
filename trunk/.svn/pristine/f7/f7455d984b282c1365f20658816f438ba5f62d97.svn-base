package com.zongteng.ztetl.api

import com.zongteng.ztetl.util.HQLFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


/**
 * 
 * 数据处理接口
 * @author panmingwen 
 *
 */

object SparkDataFrame {



  /**
    * 创建HIVE表
    */
  def createTable(spark:SparkSession,schema:String,tableName:String)
  {
    System.out.println("------------------create table start-------------------------");
    val hql = HQLFactory.getHQL(schema, tableName);
    spark.sql(hql);
    System.out.println("------------------end table start-------------------------");
  }
  

   
   
   /**
    * 删除数据
    */
   def dropTable(spark:SparkSession,schema:String,tableName:String){
     System.out.println("------------------drop  table start-------------------------");    
     val hsql="drop table if exists "+schema+"."+tableName;
     spark.sql(hsql);
      System.out.println("------------------drop table end-------------------------");   
   }
  
  
  /**
   * 保存结果到HIVE
   */
  def save(data:DataFrame,schema:String,tableName:String){
      data.write.insertInto(schema+"."+tableName);
  }
  
  /**
   * 保存结果到HIVE 
   * SaveMode :SaveMode.Append  SaveMode.Overwrite  SaveMode.Ignore
   * 
   */
  def save(data:DataFrame,schema:String,tableName:String,mode:SaveMode){
        if(mode.equals(SaveMode.Overwrite)){
            dropTable(data.sparkSession,schema,tableName);
        }
       createTable(data.sparkSession, schema, tableName)
        data.write.mode(mode).insertInto(schema+"."+tableName);

       
  }
}