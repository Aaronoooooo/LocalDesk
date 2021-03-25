package com.zongteng.ztetl.test

import com.zongteng.ztetl.api.SparkConfig
import org.apache.spark.sql.functions.broadcast
object fact_receiving_detail_test2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.init("DW_W_bil_income_F")
    try {
      //将表作为临时表加载到内存
      val par_valsql="select * from dw.par_val_list as pvl where pvl.datasource_num_id in('9001','9022')"

    val par_valdf=spark.sql(par_valsql).toDF()
      par_valdf.createOrReplaceTempView("temp_par_val_list")
      spark.table("temp_par_val_list").cache()




    val zy_wms_valsql="select row_wid,warehouse_id, warehouse_code from ods.zy_wms_warehouse"
    val zy_wms_valdf=spark.sql(zy_wms_valsql).toDF()
      zy_wms_valdf.createGlobalTempView("zy_wms_warehouse")
      spark.table("zy_wms_valdf").cache()

    //广播方式

      val bradcast_tmp=broadcast(spark.table("temp_par_val_list"))
      bradcast_tmp.join(spark.table("records"), "key").show()


    } catch {

      case ex: Exception => ex.getMessage

    } finally {
      spark.stop()
    }
  }

    }
