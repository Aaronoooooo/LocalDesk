package com.zongteng.ztetl.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object Dw_par_val_list_cache {

  /**
    * 临时表名
    */
  val TEMP_PAR_VAL_LIST_NAME = "temp_par_val_list"

  /**
    * 不缓存值列表
    */
  val EMPTY_PAR_VAL_LIST = Array("0")

  /**
    * 全部缓存
    *
    */
  val ALL_PAR_VAL_LIST = Array("ALL")

  /**
    *
    * @param sparkSession
    * @param datasource_num_id_s(9001, 9004, 9022) 通过这个参数缓存不同数据库的缓存；
    *                           1、如果全部缓存，那么传入参数Array("ALL")
    *                           2、如果不缓存，那么传入参数为Array("0")
    *                           3、如果相对应的缓存，那么传入参数为Array(对应的系统编号) Array("9001", "9004")
    */
  def cacheParValList(sparkSession: SparkSession, datasource_num_id_s: Array[String]) = {

    var cacheResult = false

    // 将表作为临时表加载到内存，缓存起来
    if (datasource_num_id_s != null && datasource_num_id_s.length > 0) {
      val result = if (datasource_num_id_s.length == 1 && "ALL".equals(datasource_num_id_s(0))) {
        sparkSession.sql("select * from dw.par_val_list")
      } else {
        sparkSession.sql(s"select * from dw.par_val_list as pvl where pvl.datasource_num_id in(${datasource_num_id_s.mkString(",")})")
      }

      // 广播变量，分发到每个Execute。不用每个task都拷贝一份
      val dfResult: DataFrame = sparkSession.sparkContext.broadcast(result.toDF()).value

      dfResult.createOrReplaceTempView(TEMP_PAR_VAL_LIST_NAME)
      sparkSession.table(TEMP_PAR_VAL_LIST_NAME).cache()

      cacheResult = true
    }

    cacheResult
  }
}
