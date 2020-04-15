package com.zongteng.ztetl.util.stream

import java.io.FileOutputStream

import com.zongteng.ztetl.entity.gc_oms.Receiving
import org.apache.commons.lang.StringUtils


object CaseClassCreateUtil {

  // 样例类所在路径（路径 + 表名）列如：F:\\project\\1.scala 注意别填错
  val path = "F:\\project\\IdeaProjects2\\trunk\\src\\main\\scala\\com\\zongteng\\ztetl\\entity\\\\"

  // mysql的表名转成驼峰命名法
  val mysql_talbe = "Receiving"

  val case_class_path = s"$path$mysql_talbe.scala"

  // mysql建表
  val mysql_create_ddl = " `receiving_id` int(11) NOT NULL AUTO_INCREMENT,\n  `sm_code` varchar(32) NOT NULL DEFAULT '',\n  `receiving_code` varchar(32) NOT NULL DEFAULT '',\n  `shipping_method` varchar(100) DEFAULT NULL,\n  `tracking_number` varchar(32) DEFAULT '',\n  `po_code` varchar(32) NOT NULL,\n  `reference_no` varchar(255) NOT NULL DEFAULT '',\n  `warehouse_id` int(11) NOT NULL DEFAULT '0',\n  `warehouse_code` varchar(30) NOT NULL DEFAULT '',\n  `transit_warehouse_id` int(11) NOT NULL DEFAULT '0',\n  `transit_warehouse_code` varchar(30) NOT NULL,\n  `transit_type` tinyint(1) NOT NULL DEFAULT '0',\n  `supplier_id` int(11) DEFAULT '0',\n  `receiving_update_user` int(11) NOT NULL DEFAULT '0',\n  `receiving_add_user` int(11) NOT NULL DEFAULT '0',\n  `customer_id` int(11) NOT NULL DEFAULT '0',\n  `customer_code` varchar(10) NOT NULL DEFAULT '',\n  `receiving_type` tinyint(1) NOT NULL DEFAULT '0',\n  `receiving_status` tinyint(1) NOT NULL DEFAULT '0',\n  `receiving_source_type` tinyint(1) NOT NULL DEFAULT '0',\n  `contacter` varchar(32) NOT NULL DEFAULT '',\n  `contact_phone` varchar(32) NOT NULL DEFAULT '',\n  `receiving_description` varchar(1000) DEFAULT NULL,\n  `receiving_add_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',\n  `receiving_update_time` datetime DEFAULT '0000-00-00 00:00:00',\n  `refercence_form_id` varchar(20) NOT NULL DEFAULT '0',\n  `ie_port` varchar(50) NOT NULL DEFAULT '',\n  `form_type` varchar(50) NOT NULL DEFAULT '',\n  `traf_name` varchar(255) NOT NULL DEFAULT '',\n  `wrap_type` varchar(50) NOT NULL DEFAULT '',\n  `pack_no` smallint(2) NOT NULL DEFAULT '0',\n  `traf_mode` varchar(50) NOT NULL DEFAULT '',\n  `trade_mode` varchar(50) NOT NULL DEFAULT '',\n  `trans_mode` varchar(50) NOT NULL DEFAULT '',\n  `conta_id` varchar(50) NOT NULL DEFAULT '',\n  `conta_model` varchar(100) NOT NULL DEFAULT '',\n  `conta_wt` float(8,2) NOT NULL DEFAULT '0.00',\n  `haveconta` tinyint(1) NOT NULL DEFAULT '0',\n  `eda_date` date DEFAULT '0000-00-00',\n  `expected_date` date DEFAULT '0000-00-00',\n  `receiving_transfer_status` tinyint(1) NOT NULL DEFAULT '0',\n  `receiving_exception` tinyint(1) NOT NULL DEFAULT '0',\n  `receiving_exception_handle` tinyint(1) NOT NULL DEFAULT '0',\n  `receiving_abnormal_confirm` tinyint(1) NOT NULL DEFAULT '0',\n  `tax_type` varchar(32) DEFAULT NULL,\n  `insurance_type` varchar(32) DEFAULT NULL,\n  `customer_type` varchar(32) DEFAULT NULL,\n  `wms_qty_confirm` tinyint(1) NOT NULL DEFAULT '0',\n  `income_type` int(1) NOT NULL DEFAULT '0',\n  `region_0` int(10) DEFAULT NULL,\n  `region_1` int(10) DEFAULT NULL,\n  `region_2` int(10) DEFAULT NULL,\n  `street` varchar(300) DEFAULT NULL,\n  `box_total` int(10) DEFAULT '1',\n  `sku_total` int(10) DEFAULT '0',\n  `sku_species` int(10) DEFAULT '0',\n  `check_status` tinyint(1) NOT NULL DEFAULT '0' COMMENT '数据核对状态',\n  `is_fba` tinyint(1) DEFAULT '0',\n  `create_type` char(5) DEFAULT 'hand' COMMENT '创建入库单类型 hand 系统创建 api',\n  `date_release` datetime DEFAULT NULL,\n  `receiving_shipping_type` tinyint(1) DEFAULT NULL COMMENT '货运方式，0：空运，1：海运，2：快递',\n  `received_packages` int(11) DEFAULT '0' COMMENT '到货箱数',\n  `container_number` varchar(32) DEFAULT '' COMMENT '海柜号',\n  `first_sign_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '首次签收时间',\n  `last_sign_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '最近一次签收时间',\n  `rv_sign_batch` smallint(10) NOT NULL DEFAULT '0' COMMENT '入库单签收批次',\n  `rv_sign_status` tinyint(1) NOT NULL DEFAULT '0' COMMENT '入库单签收状态  0-未签收，1-签收中，2-签收完成',\n  `new_add_sku_species` int(11) unsigned DEFAULT '0' COMMENT '新增预报SKU数量',\n  `volume` decimal(10,2) DEFAULT NULL,\n  `weight` decimal(10,2) DEFAULT NULL,\n  `r_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',\n  `is_change_sku` tinyint(1) DEFAULT '1' COMMENT '是否换标,1:不换标，2:换标',\n  `is_third` tinyint(1) DEFAULT '1' COMMENT '是否第三方,1:非第三方，2:第三方',\n  `take_stock_code` char(2) NOT NULL DEFAULT '0' COMMENT '箱唛标识码',"

  def main(args: Array[String]): Unit = {
    //CaseClassCreateUtil()
    //PrintFields()

    println(Receiving.getClass.getClassLoader.getResource("CaseClassCreateUtil").getPath)
  }

  /**
    * 生成对应的样例类
    */
  def CaseClassCreateUtil() = {

    // 打印到控制台
    println(s"case class $mysql_talbe (")
    val sqlStrings: Array[String] = mysql_create_ddl.split("\n").map((e: String) => {
      e.replaceAll("` .*,$|`", "").concat(": String,").trim
    })

    // 去掉最后一行的逗号
    val last: String = sqlStrings.last
    sqlStrings(sqlStrings.length - 1) = last.substring(0, last.lastIndexOf(","))
    sqlStrings.foreach(println(_))

    println(")")
    println("字段个数 == " + sqlStrings.length)

    // 输出到指定文件中
    val stream = new FileOutputStream(case_class_path)
    stream.write(s"case class $mysql_talbe (".getBytes())
    sqlStrings.foreach((sql: String) => {
      stream.write(sql.getBytes())
      stream.write("\r\n".getBytes()) // 换行
    })

    stream.write(")".getBytes())

    stream.close()
  }


  //val timeFields: Array[String] = Array[String]("so_add_time","so_ship_time","so_delivered_time","so_update_time","so_timestamp")
  //val defaultNullInts = Array[String]()
  //val defaultNullStrings = Array[String]()
  /**
    * 获取对应的时间类型
    *
    */
  def PrintFields() = {


    // timeFieldSstr 空格datetime.*,$|空格timestamp.*,$|空格date.*,$  timestamp,

    val sb = new StringBuilder
    mysql_create_ddl.split("\n")
      .filter(_.matches(".* datetime.*,$|.* timestamp.*,$|.* date.*,$"))
      .map("\"" + _.replaceAll("` .*,$|`", "").trim + "\", ")
      .addString(sb)

    val timeFields: String = if(StringUtils.isNotBlank(sb.toString())) sb.substring(0, sb.lastIndexOf(",")) else ""

    println(s"val timeFields: Array[String] = Array[String](${timeFields})")

    // defaultNullInts DEFAULT NULL INT

    val sb2 = new StringBuilder

    mysql_create_ddl.split("\n")
      .filter((e:String) => e.contains("DEFAULT NULL") && e.matches(".* int\\(.*,$|.* tinyint\\(.*,$|.* decimal\\(.*,$"))
      .map("\"" + _.replaceAll("` .*,$|`", "").trim + "\",")
      .addString(sb2)

    val defaultNullInts: String = if (StringUtils.isNotBlank(sb2.toString())) sb2.substring(0, sb2.lastIndexOf(",")) else ""

    println(s"val defaultNullInts = Array[String]($defaultNullInts)")


    // defaultNullStrings DEFAULT NULL varchar char

    val sb3 = new StringBuilder

    mysql_create_ddl.split("\n")
      .filter((e:String) => e.contains("DEFAULT NULL") && e.matches(".* varchar\\(.*,$|.* char\\(.*,$"))
      .map("\"" + _.replaceAll("` .*,$|`", "").trim + "\",")
      .addString(sb3)

    val defaultNullStrings: String = if (StringUtils.isNotBlank(sb3.toString())) sb3.substring(0, sb3.lastIndexOf(",")) else ""

    println(s"val defaultNullStrings = Array[String]($defaultNullStrings)")


  }

}
