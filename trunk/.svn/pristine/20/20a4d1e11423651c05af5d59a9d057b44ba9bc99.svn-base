package com.zongteng.ztetl.util.stream

import com.zongteng.ztetl.common.SystemCodeUtil
import com.zongteng.ztetl.entity.common.AssistParameter
import org.apache.commons.lang.StringUtils

/**
  * gc_owms库的表通过数据名和表名获取对应的辅助实体类（样例类）
  */
object GcOwmsAssistParameterUtil {
  /**
    * @param database 数据库
    * @param table    表名
    */
  def getEntityObject(database: String, table: String) = {

    assert(StringUtils.isNotBlank(database), "数据库不能为空")
    assert(StringUtils.isNotBlank(table), "表名不能为空")

    val dt: String = database + "." + table

    dt match {
      //订单(orders)
      case "owms_uk.order_address_book" => owms_uk$order_address_book(database, table)
      case "owms_ukob.order_address_book" => owms_ukob$order_address_book(database, table)
      case "owms_us_east.order_address_book" => owms_us_east$order_address_book(database, table)
      case "owms_usnb.order_address_book" => owms_usnb$order_address_book(database, table)
      case "owms_usot.order_address_book" => owms_usot$order_address_book(database, table)
      case "owms_ussc.order_address_book" => owms_ussc$order_address_book(database, table)
      case "owms_us_west.order_address_book" => owms_us_west$order_address_book(database, table)
      case "owms_au.order_address_book" => owms_au$order_address_book(database, table)
      case "owms_cz.order_address_book" => owms_cz$order_address_book(database, table)
      case "owms_de.order_address_book" => owms_de$order_address_book(database, table)
      case "owms_es.order_address_book" => owms_es$order_address_book(database, table)
      case "owms_frvi.order_address_book" => owms_frvi$order_address_book(database, table)
      case "owms_it.order_address_book" => owms_it$order_address_book(database, table)
      case "owms_jp.order_address_book" => owms_jp$order_address_book(database, table)
      case "owms_uk.orders" => owms_uk$orders(database, table)
      case "owms_ukob.orders" => owms_ukob$orders(database, table)
      case "owms_us_east.orders" => owms_us_east$orders(database, table)
      case "owms_usnb.orders" => owms_usnb$orders(database, table)
      case "owms_usot.orders" => owms_usot$orders(database, table)
      case "owms_ussc.orders" => owms_ussc$orders(database, table)
      case "owms_us_west.orders" => owms_us_west$orders(database, table)
      case "owms_au.orders" => owms_au$orders(database, table)
      case "owms_cz.orders" => owms_cz$orders(database, table)
      case "owms_de.orders" => owms_de$orders(database, table)
      case "owms_es.orders" => owms_es$orders(database, table)
      case "owms_frvi.orders" => owms_frvi$orders(database, table)
      case "owms_it.orders" => owms_it$orders(database, table)
      case "owms_jp.orders" => owms_jp$orders(database, table)
      //出库(picking)
      case "owms_uk.picking_physical_relation" => owms_uk$picking_physical_relation(database, table)
      case "owms_ukob.picking_physical_relation" => owms_ukob$picking_physical_relation(database, table)
      case "owms_us_east.picking_physical_relation" => owms_us_east$picking_physical_relation(database, table)
      case "owms_usnb.picking_physical_relation" => owms_usnb$picking_physical_relation(database, table)
      case "owms_usot.picking_physical_relation" => owms_usot$picking_physical_relation(database, table)
      case "owms_ussc.picking_physical_relation" => owms_ussc$picking_physical_relation(database, table)
      case "owms_us_west.picking_physical_relation" => owms_us_west$picking_physical_relation(database, table)
      case "owms_au.picking_physical_relation" => owms_au$picking_physical_relation(database, table)
      case "owms_cz.picking_physical_relation" => owms_cz$picking_physical_relation(database, table)
      case "owms_de.picking_physical_relation" => owms_de$picking_physical_relation(database, table)
      case "owms_es.picking_physical_relation" => owms_es$picking_physical_relation(database, table)
      case "owms_frvi.picking_physical_relation" => owms_frvi$picking_physical_relation(database, table)
      case "owms_it.picking_physical_relation" => owms_it$picking_physical_relation(database, table)
      case "owms_jp.picking_physical_relation" => owms_jp$picking_physical_relation(database, table)
      case "owms_uk.wellen_area" => owms_uk$wellen_area(database, table)
      case "owms_ukob.wellen_area" => owms_ukob$wellen_area(database, table)
      case "owms_us_east.wellen_area" => owms_us_east$wellen_area(database, table)
      case "owms_usnb.wellen_area" => owms_usnb$wellen_area(database, table)
      case "owms_usot.wellen_area" => owms_usot$wellen_area(database, table)
      case "owms_ussc.wellen_area" => owms_ussc$wellen_area(database, table)
      case "owms_us_west.wellen_area" => owms_us_west$wellen_area(database, table)
      case "owms_au.wellen_area" => owms_au$wellen_area(database, table)
      case "owms_cz.wellen_area" => owms_cz$wellen_area(database, table)
      case "owms_de.wellen_area" => owms_de$wellen_area(database, table)
      case "owms_es.wellen_area" => owms_es$wellen_area(database, table)
      case "owms_frvi.wellen_area" => owms_frvi$wellen_area(database, table)
      case "owms_it.wellen_area" => owms_it$wellen_area(database, table)
      case "owms_jp.wellen_area" => owms_jp$wellen_area(database, table)
      case "owms_uk.wellen_log" => owms_uk$wellen_log(database, table)
      case "owms_ukob.wellen_log" => owms_ukob$wellen_log(database, table)
      case "owms_us_east.wellen_log" => owms_us_east$wellen_log(database, table)
      case "owms_usnb.wellen_log" => owms_usnb$wellen_log(database, table)
      case "owms_usot.wellen_log" => owms_usot$wellen_log(database, table)
      case "owms_ussc.wellen_log" => owms_ussc$wellen_log(database, table)
      case "owms_us_west.wellen_log" => owms_us_west$wellen_log(database, table)
      case "owms_au.wellen_log" => owms_au$wellen_log(database, table)
      case "owms_cz.wellen_log" => owms_cz$wellen_log(database, table)
      case "owms_de.wellen_log" => owms_de$wellen_log(database, table)
      case "owms_es.wellen_log" => owms_es$wellen_log(database, table)
      case "owms_frvi.wellen_log" => owms_frvi$wellen_log(database, table)
      case "owms_it.wellen_log" => owms_it$wellen_log(database, table)
      case "owms_jp.wellen_log" => owms_jp$wellen_log(database, table)

      case _ => throw new Exception("数据库 {" + database + "} 中表{" + table + "} 对应的方法不存在,请添加")
    }
  }

  def owms_uk$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uk.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UK
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ukob$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ukob.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UKOB
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_east$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usea.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USEA
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usnb$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usnb.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USNB
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usot$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usot.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USOT
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ussc$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ussc.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USSC
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_west$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uswe.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USWE
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_au$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_au.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_AU
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_cz$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_cz.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_CZ
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_de$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_de.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_DE
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_es$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_es.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_ES
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_frvi$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_frvi.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_FRVI
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_it$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_it.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_IT
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_jp$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_jp.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_JP
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }
  def owms_uk$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uk.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UK
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ukob$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ukob.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UKOB
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_east$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usea.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USEA
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usnb$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usnb.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USNB
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usot$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usot.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USOT
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ussc$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ussc.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USSC
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_west$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uswe.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USWE
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_au$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_au.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_AU
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_cz$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_cz.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_CZ
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_de$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_de.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_DE
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_es$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_es.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_ES
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_frvi$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_frvi.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_FRVI
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_it$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_it.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_IT
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_jp$orders(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_jp.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_JP
    val add_time_field = "add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }




  //picking
  def owms_uk$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uk.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UK
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ukob$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ukob.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UKOB
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_east$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usea.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USEA
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usnb$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usnb.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USNB
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usot$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usot.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USOT
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ussc$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ussc.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USSC
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_west$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uswe.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USWE
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_au$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_au.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_AU
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_cz$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_cz.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_CZ
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_de$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_de.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_DE
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_es$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_es.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_ES
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_frvi$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_frvi.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_FRVI
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_it$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_it.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_IT
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_jp$picking_physical_relation(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_jp.PickingPhysicalRelation]
    val primaryKey: String = "ppr_id"
    val cf: String = "picking_physical_relation"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_JP
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_uk$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uk.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UK
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ukob$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ukob.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UKOB
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_east$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usea.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USEA
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usnb$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usnb.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USNB
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usot$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usot.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USOT
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ussc$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ussc.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USSC
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_west$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uswe.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USWE
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_au$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_au.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_AU
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_cz$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_cz.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_CZ
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_de$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_de.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_DE
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_es$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_es.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_ES
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_frvi$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_frvi.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_FRVI
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_it$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_it.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_IT
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_jp$wellen_area(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_jp.WellenArea]
    val primaryKey: String = "wellen_area_id"
    val cf: String = "wellen_area"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_JP
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_uk$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uk.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UK
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ukob$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ukob.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_UKOB
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_east$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usea.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USEA
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usnb$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usnb.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USNB
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_usot$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_usot.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USOT
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_ussc$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_ussc.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USSC
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_us_west$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_uswe.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_USWE
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_au$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_au.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_AU
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_cz$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_cz.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_CZ
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_de$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_de.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_DE
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_es$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_es.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_ES
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_frvi$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_frvi.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_FRVI
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_it$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_it.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_IT
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def owms_jp$wellen_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_owms.gc_owms_jp.WellenLog]
    val primaryKey: String = "wellen_log_id"
    val cf: String = "wellen_log"
    val datasource_num_id: String = SystemCodeUtil.GC_OWMS_JP
    val add_time_field = "create_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def check(assistParameter: AssistParameter) = {
    assert(assistParameter.className != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的样例类不存在,请添加")
    assert(assistParameter.primaryKey != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的主键不存在,请添加")
    assert(assistParameter.cf != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的列族不存在,请添加")
    assert(assistParameter.datasource_num_id != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的数据来源不存在,请添加")
    assert(assistParameter.add_time_field != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的新增时间不存在,请添加")
    assert(assistParameter.update_time_field_1 != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的修改时间1_时间戳不存在,请添加")
    assert(assistParameter.update_time_field_2 != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的修改时间2_修改时间不存在,请添加")
    assistParameter
  }
}
