package com.zongteng.ztetl.etl.stream.gc_lms
import com.zongteng.ztetl.api.SparkConfig
import com.zongteng.ztetl.util.{DateUtil, Log}
import org.apache.spark.sql.SparkSession

object Insert_stream_gc_lms_container_details {

  // 任务名称(一般同类名)
  private val task = "Insert_stream_gc_lms_container_details"

  // ods层类名
  private val tableName = "stream_gc_lms_container_details"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  val sql = "SELECT\n" +
    "    concat(datasource_num_id, container_details_id) as row_wid,\n" +
    "    date_format(current_date(), 'yyyyMMdd') as etl_proc_wid,\n" +
    "    current_timestamp ( ) as w_insert_dt,\n" +
    "    current_timestamp ( ) as w_update_dt,\n" +
    "    datasource_num_id as datasource_num_id,\n" +
    "    data_flag as data_flag,\n" +
    "    integration_id as integration_id,\n" +
    "    created_on_dt as created_on_dt,\n" +
    "    changed_on_dt as changed_on_dt,\n" +
    "\n" +
    "    container_details_id,\n" +
    "    container_id,\n" +
    "    container_code,\n" +
    "    order_number,\n" +
    "    tracking_number,\n" +
    "    channel_code,\n" +
    "    loader_time,\n" +
    "    shipper_time,\n" +
    "    day\n" +
    "FROM ods.stream_gc_lms_database_container_details"

  def main(args: Array[String]): Unit = {

    val sqlArray: Array[String] = Array("au", "cz", "es", "frvi", "it", "uk", "uswe", "usea", "ussc").map(sql.replace("database", _))

    val logTask: String = Log.start(task)

    try {
      val spark: SparkSession = SparkConfig.init(task)


      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") // 设置动态分区为非严格模式
      spark.sql("set hive.exec.dynamic.partition=true") // 开启动态分区

      // 将容器详情9张表合成一张表，使用INSERT INTO
      val insertStr = s"INSERT INTO TABLE ods.$tableName PARTITION(day) \n"

      sqlArray.map(insertStr + _).foreach(spark.sql(_))

      spark.stop()
      Log.end(logTask)
    } catch {
      case ex: Exception => Log.error(logTask, ex)
    }

  }

}
