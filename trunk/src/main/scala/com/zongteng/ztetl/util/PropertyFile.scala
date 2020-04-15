package com.zongteng.ztetl.util

import java.util.Properties;



object PropertyFile {
  
  val PROP_FILE = "spark.properties";
  
  var properties:Properties=null;
  
  /**
   * 加载属性文件
   */
  def readPropertyFile()
  {
    try
    {
        val props = new Properties();
        val is = getClass().getClassLoader().getResourceAsStream("spark.properties");
        if (is == null) {
          throw new RuntimeException("Not found property file: spark.properties");
        }
        props.load(is);
        properties = props;
    }catch{
          case ex:Exception  => ex.getMessage
      }
  }
  
  /**
  * 读属性值
  */
   def getProperty(key: String):String={
     if (properties == null){
       readPropertyFile();
     }
    return properties.getProperty(key);
  }
  
}