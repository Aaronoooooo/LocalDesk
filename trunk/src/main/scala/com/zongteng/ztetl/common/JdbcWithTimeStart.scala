package com.zongteng.ztetl.common

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import com.zongteng.ztetl.util.EtlIncLog
import org.apache.commons.lang3.StringUtils

object JdbcWithTimeStart {
  var start_time=""
  var end_time=""
  var id=""

  /**
    * fact表dw层运行开始,插入数据到数据库中,并查出开始和结束时间
    * @return
    */
  def getstart_mysql() = {
    val task = "GC_ETL_INC"
    val before_day = 1
    //向数据库中插入一条记录,并获取id值
    val id_db = EtlIncLog.start(task, before_day)
    try {
      if (StringUtils.isBlank(id_db) || "".equalsIgnoreCase(id_db)) {
        throw new Exception("插入数据不成功")
      }

      val startAndEndTiem: String = EtlIncLog.GetStartEndTime(id_db)
      val strings: Array[String] = startAndEndTiem.split(",")
      val start_time_db = strings(0)
      val end_time_db = strings(1)
      //加载路径
      val file: File = new File(this.getClass.getResource("/etl_inc_log.properties").getPath)
      if (file == null) {
        throw new RuntimeException("Not found property file: etl_inc_log.properties");
      }

      //设置输出变量
      val stream = new FileOutputStream(file)

      stream.write(s"start_time=$start_time_db".getBytes)
      stream.write("\r\n".getBytes())
      stream.write(s"end_time=$end_time_db".getBytes)
      stream.write("\r\n".getBytes())
      stream.write(s"id=$id_db".getBytes)
      //刷新
      stream.flush()

      val properties = new Properties()
      val streamIn = new FileInputStream(file)
      properties.load(streamIn)
      start_time= properties.getProperty("start_time")
      end_time = properties.getProperty("end_time")
      id = properties.getProperty("id")
      println(start_time)
      println(end_time)
      println(id)

    } catch {
      case ex: Exception => ex.getMessage
    }

  }
  /**
    * fact表dw层运行结束,修改记录
    * @return
    */

  def getEnd_mysql() = {
    try{
      /*if(id==null || "".equalsIgnoreCase(id))
      {
        val properties = new Properties()
        properties.load(new FileInputStream(new File(this.getClass.getResource("/etl_inc_log.properties").getPath)))

        //加载id值
        id = properties.getProperty("id")
      }*/
      EtlIncLog.end(id)

    }catch{
      case  ex:Exception=>EtlIncLog.error(id,ex)
    }


  }




  def main(args: Array[String]): Unit = {


   /* //加载路径
    val file: File = new File(this.getClass.getResource("/etl_inc_log.properties").getPath)
    if (file == null) {
      throw new RuntimeException("Not found property file: etl_inc_log.properties");
    }

    //设置输出变量
    val stream = new FileOutputStream(file)

    stream.write(s"start_time=$start_time".getBytes)
    stream.write("\r\n".getBytes())
    stream.write(s"end_time=$end_time".getBytes)
    stream.write("\r\n".getBytes())
    stream.write(s"id=$id".getBytes)
    //刷新
    stream.flush()

    //设置输入变量
    val properties = new Properties()
    val stream2 = new FileInputStream(file)
    properties.load(stream2)
    val start: String = properties.getProperty("start_time")
    val end: String = properties.getProperty("end_time")
    val id_code: String = properties.getProperty("id")
    println(start)
    println(end)
    println(id_code)*/
    getstart_mysql()
  }


}
