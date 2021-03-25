package com.zongteng.ztetl.util.stream

import java.io.FileOutputStream

import scala.collection.mutable.ListBuffer

object DwAzkabanScriptUtil {


  def main(args: Array[String]): Unit = {

    // 入库单所有实时模块的全类名
    val fullClassNames = Array[String](
      //      "com.zongteng.ztetl.stream.ods.oms_goodcang_com.Ods_gc_oms_receiving_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.oms_goodcang_com.Ods_gc_oms_receiving_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_receiving_batch_inc",
      //      //"com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_bil_income_attach_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_gc_putaway_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_gc_receiving_box_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_gc_receiving_box_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_receiving_box_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_gc_receiving_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_gc_receiving_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_gc_receiving_log_inc",
      //      //"com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_receiving_batch_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_putaway_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_putaway_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_receiving_box_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_receiving_detail_batch_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_receiving_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_receiving_inc",
      //      "com.zongteng.ztetl.stream.ods.wms_goocang_com.Ods_gc_wms_receiving_log_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_putaway_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_putaway_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_receiving_box_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_receiving_box_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_receiving_detail_batch_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_receiving_detail_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_receiving_inc",
      //      "com.zongteng.ztetl.stream.ods.zy_wms.Ods_zy_wms_receiving_log_inc"
      "com.zongteng.ztetl.stream.ods.oms_goodcang_com.SparkStream_gc_oms",
      "com.zongteng.ztetl.stream.ods.wms_goocang_com.SparkStream_gc_wms",
      "com.zongteng.ztetl.stream.ods.zy_wms.SparkStream_zy_wms",
      "com.zongteng.ztetl.etl.ods.SparkStream_gc_wms_ibl"
    )

    // 脚本存储的路径
    val path = "F:\\workDoucument\\svn\\Delivery\\E 开发实现\\SourceCode\\etl\\2、receiving\\ods\\fact_azkaban"

    // azkaban脚本的命令规则（spark_stream_ods_ods层表名_inc.job）
    val azkaban_job_name = "spark_stream_odsTableName_inc"

    val spark_submit_script = "type=command\n" +
      "dependencies=end_sqoop\n" +
      "command=spark2-submit --master yarn --class $fullClassName --driver-memory 10g --executor-memory 10g --executor-cores 8 --num-executors 10 --deploy-mode cluster /ztscript/ztetl-1.0-SNAPSHOT2.jar"


    val end_stream_mysql_ods = "type=command\n" +
      "dependencies=$stream_script\n" +
      "command=echo '实时导入数据结束'\n" +
      "command.1= sleep 5"

    val stream_script: ListBuffer[String] = ListBuffer[String]()

    var i = 0;

    fullClassNames.foreach((fcl: String) => {
      val fcl2 = fcl
      val className = fcl.substring(fcl.lastIndexOf(".") + 1)
      val azkaban_script_name = azkaban_job_name.replace("odsTableName", className).concat(".job").toLowerCase
      val azkaban_script_name2 = azkaban_job_name.replace("odsTableName", className).toLowerCase
      val azkaban_script_content = spark_submit_script.replace("$fullClassName", fcl2)

      println(s"第${i + 1}个脚本内容 \n" + azkaban_script_content)
      println()

      stream_script += azkaban_script_name2

      var outStream: FileOutputStream = null

      try {
        outStream = new FileOutputStream(path + "\\" + azkaban_script_name)
        outStream.write(azkaban_script_content.getBytes())
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        outStream.close()
      }

      i += 1;
    })


    var outStream2: FileOutputStream = null

    try {
      outStream2 = new FileOutputStream(path + "\\" + "end_stream_mysql_ods.job")
      val content: String = end_stream_mysql_ods.replace("$stream_script", stream_script.mkString(","))

      println("实时结束节点：" + content)
      outStream2.write(content.getBytes())
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      outStream2.close()
    }


    println("一共生成" + (i + 1) + "个脚本")
  }


}
