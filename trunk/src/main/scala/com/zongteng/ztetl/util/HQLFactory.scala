package com.zongteng.ztetl.util

import scala.xml.XML


object HQLFactory {
  
  
  
      /**
     * 获取 CREATE TABLE SQL
     */
    def getHQL(schema:String,tableName:String):String={
     try{    
      val  xmlString = XML.loadFile(PropertyFile.getProperty("hiveTable"));
      val  hsql=(xmlString\\"hql").filter(x=>((x\"@tableName").text)==tableName)
           .filter(x=>((x\"@schema").text)==schema).text
      return hsql;
     }catch{
       case ex:Exception=>errorMesage(ex);
     }
    }
  
    
 def errorMesage(ex:Exception):String={
   return ex.getMessage;
}

  
  
  def main(args:Array[String]){
       var sql=HQLFactory.getHQL("etl", "etl_cs");
        System.out.println(sql)
      
    }
  
}