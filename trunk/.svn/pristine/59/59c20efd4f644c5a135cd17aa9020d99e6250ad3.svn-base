package com.zongteng.ztetl.common

import java.io.{File, FileInputStream}
import java.util.Properties
import com.zongteng.ztetl.common.JdbcWithTimeStart.id

import com.zongteng.ztetl.util.EtlIncLog

object JdbcWithTimeEnd {
  def main(args: Array[String]): Unit = {
    try{
      if(id==null || "".equalsIgnoreCase(id))
        {
          val properties = new Properties()
          properties.load(new FileInputStream(new File(this.getClass.getResource("/etl_inc_log.properties").getPath)))

          //加载id值
          id = properties.getProperty("id")
        }
      EtlIncLog.end(id)

    }catch{
      case  ex:Exception=>EtlIncLog.error(id,ex)
    }

  }

}
