package com.zongteng.ztetl.util.stream

import com.zongteng.ztetl.common.SystemCodeUtil
import com.zongteng.ztetl.entity.common.AssistParameter
import org.apache.commons.lang.StringUtils

/**
  * 通过数据名和表名获取对应的辅助实体类（样例类）
  */
object AssistParameterUtil {

  /**
    * @param database 数据库
    * @param table    表名
    */
  def getEntityObject(database: String, table: String) = {

    assert(StringUtils.isNotBlank(database), "数据库不能为空")
    assert(StringUtils.isNotBlank(table), "表名不能为空")

    val dt: String = database + "." + table

    dt match {
      //入库单(receiving)
      case "wms_goodcang_com.putaway" => wms_goodcang_com$putaway(database, table)
      case "wms.putaway" => wms$putaway(database, table)
      case "wms_goodcang_com.gc_putaway_detail" => wms_goodcang_com$gc_putaway_detail(database, table)
      case "wms_goodcang_com.putaway_detail" => wms_goodcang_com$putaway_detail(database, table)
      case "wms.putaway_detail" => wms$putaway_detail(database, table)
      case "wms_goodcang_com.gc_receiving" => wms_goodcang_com$gc_receiving(database, table)
      case "wms_goodcang_com.receiving" => wms_goodcang_com$receiving(database, table)
      case "wms.receiving" => wms$receiving(database, table)
      case "oms_goodcang_com.receiving" => oms_goodcang_com$receiving(database, table)
      case "wms_goodcang_com.gc_receiving_detail" => wms_goodcang_com$gc_receiving_detail(database, table)
      case "wms_goodcang_com.receiving_detail" => wms_goodcang_com$receiving_detail(database, table)
      case "wms.receiving_detail" => wms$receiving_detail(database, table)
      case "oms_goodcang_com.receiving_detail" => oms_goodcang_com$receiving_detail(database, table)
      case "wms_goodcang_com.gc_receiving_box" => wms_goodcang_com$gc_receiving_box(database, table)
      case "wms_goodcang_com.receiving_box" => wms_goodcang_com$receiving_box(database, table)
      case "wms.receiving_box" => wms$receiving_box(database, table)
      case "wms_goodcang_com.gc_receiving_box_detail" => wms_goodcang_com$gc_receiving_box_detail(database, table)
      case "wms_goodcang_com.receiving_box_detail" => wms_goodcang_com$receiving_box_detail(database, table)
      case "wms.receiving_box_detail" => wms$receiving_box_detail(database, table)
      case "wms_goodcang_com.gc_receiving_log" => wms_goodcang_com$gc_receiving_log(database, table)
      case "wms_goodcang_com.receiving_log" => wms_goodcang_com$receiving_log(database, table)
      case "wms.receiving_log" => wms$receiving_log(database, table)
      case "wms_goodcang_com.gc_receiving_batch" => wms_goodcang_com$gc_receiving_batch(database, table)
      case "wms_goodcang_com.receiving_detail_batch" => wms_goodcang_com$receiving_detail_batch(database, table)
      case "wms.receiving_detail_batch" => wms$receiving_detail_batch(database, table)
      case "wms_goodcang_com.quality_control" => wms_goodcang_com$quality_control(database, table)
      case "wms.quality_control" => wms$quality_control(database, table)
      //订单(orders)
      case "wms_goodcang_com.order_log" => wms_goodcang_com$order_log(database, table)
      case "wms.order_log" => wms$order_log(database, table)
      case "wms_goodcang_com.order_operation_time" => wms_goodcang_com$order_operation_time(database, table)
      case "wms.order_operation_time" => wms$order_operation_time(database, table)
      case "wms_goodcang_com.ship_order" => wms_goodcang_com$ship_order(database, table)
      case "wms.ship_order" => wms$ship_order(database, table)
      case "wms_goodcang_com.order_address_book" => wms_goodcang_com$order_address_book(database, table)
      case "wms.order_address_book" => wms$order_address_book(database, table)
      case "wms_goodcang_com.order_product" => wms_goodcang_com$order_product(database, table)
      case "wms.order_product" => wms$order_product(database, table)
      case "wms_goodcang_com.order_physical_relation" => wms_goodcang_com$order_physical_relation(database, table)
      case "wms_goodcang_com.order_product_physical_relation" => wms_goodcang_com$order_product_physical_relation(database, table)
      case "wms_goodcang_com.orders" => wms_goodcang_com$orders(database, table)
      case "wms.orders" => wms$orders(database, table)
      case "oms_goodcang_com.orders" => oms_goodcang_com$orders(database, table)
      case "wms_goodcang_com.return_orders" => wms_goodcang_com$return_orders(database, table)
      case "wms.return_orders" => wms$return_orders(database, table)
      case "oms_goodcang_com.return_orders" => oms_goodcang_com$return_orders(database, table)
      case "wms_goodcang_com.after_sales_return_orders" => wms_goodcang_com$after_sales_return_orders(database, table)
      case "wms.after_sales_return_orders" => wms$after_sales_return_orders(database, table)

      //仓储(storage)
      case "wms_goodcang_com.flow_volume" => wms_goodcang_com$flow_volume(database, table)
      case "wms_goodcang_com.inventory_batch" => wms_goodcang_com$inventory_batch(database, table)
      case "wms.inventory_batch" => wms$inventory_batch(database, table)
      case "wms_goodcang_com.inventory_difference" => wms_goodcang_com$inventory_difference(database, table)
      case "wms_goodcang_com.inventory_difference_detail" => wms_goodcang_com$inventory_difference_detail(database, table)
      case "wms_goodcang_com.product_inventory" => wms_goodcang_com$product_inventory(database, table)
      case "wms.product_inventory" => wms$product_inventory(database, table)
      case "wms_goodcang_com.take_stock" => wms_goodcang_com$take_stock(database, table)
      case "wms.take_stock" => wms$take_stock(database, table)
      case "wms_goodcang_com.take_stock_assignment" => wms_goodcang_com$take_stock_assignment(database, table)
      case "wms.take_stock_assignment" => wms$take_stock_assignment(database, table)
      case "wms_goodcang_com.take_stock_item" => wms_goodcang_com$take_stock_item(database, table)
      case "wms.take_stock_item" => wms$take_stock_item(database, table)
      case "wms_goodcang_com.inventory_batch_log" => wms_goodcang_com$inventory_batch_log(database, table)
      case "wms.inventory_batch_log" => wms$inventory_batch_log(database, table)

      case "wms_goodcang_com.bil_income_attach" => wms_goodcang_com$bil_income_attach(database, table)
      case _ => throw new Exception("数据库 {" + database + "} 中表{" + table + "} 对应的方法不存在,请添加")
    }
  }

  // 订单模块
  // 谷仓订单产品表
  def wms_goodcang_com$order_product(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.OrderProduct")
    val primaryKey: String = "op_id"
    val cf: String = "op"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "op_add_time"
    val update_time_field_1: String = "op_timestamp"
    val update_time_field_2: String = "op_update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$order_product(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.OrderProduct")
    val primaryKey: String = "op_id"
    val cf: String = "op"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "op_add_time"
    val update_time_field_1: String = "op_timestamp"
    val update_time_field_2: String = "op_update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$order_physical_relation(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.OrderPhysicalRelation")
    val primaryKey: String = "opr_id"
    val cf: String = "opr"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = ""
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$order_product_physical_relation(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.OrderProductPhysicalRelation")
    val primaryKey: String = "oppr_id"
    val cf: String = "oppr"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = ""
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }
  def wms_goodcang_com$orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "add_time"
    val update_time_field_1: String = "o_timestamp"
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "add_time"
    val update_time_field_1: String = "o_timestamp"
    val update_time_field_2: String = "update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }
  def oms_goodcang_com$orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_oms.Orders]
    val primaryKey: String = "order_id"
    val cf: String = "orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OMS
    val add_time_field: String = "date_create"
    val update_time_field_1: String = "o_timestamp"
    val update_time_field_2: String = "date_last_modify"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$return_orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.ReturnOrders]
    val primaryKey: String = "ro_id"
    val cf: String = "return_orders"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "ro_add_time"
    val update_time_field_1: String = "ro_timestamp"
    val update_time_field_2: String = "ro_update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$return_orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.ReturnOrders]
    val primaryKey: String = "ro_id"
    val cf: String = "return_orders"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "ro_add_time"
    val update_time_field_1: String = "ro_timestamp"
    val update_time_field_2: String = "ro_update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def oms_goodcang_com$return_orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_oms.ReturnOrders]
    val primaryKey: String = "ro_id"
    val cf: String = "return_orders"
    val datasource_num_id: String = SystemCodeUtil.GC_OMS
    val add_time_field: String = "ro_add_time"
    val update_time_field_1: String = "ro_timestamp"
    val update_time_field_2: String = "ro_update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$after_sales_return_orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.AfterSalesReturnOrders]
    val primaryKey: String = "asro_id"
    val cf: String = "after_sales_return_orders"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "asro_add_time"
    val update_time_field_1: String = "asro_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }
  def wms$after_sales_return_orders(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.AfterSalesReturnOrders]
    val primaryKey: String = "asro_id"
    val cf: String = "after_sales_return_orders"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "asro_add_time"
    val update_time_field_1: String = "asro_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  // fact_receiving_batch
  def wms_goodcang_com$gc_receiving_batch(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.GcReceivingBatch")
    val primaryKey: String = "rbl_id"
    val cf: String = "gc_receiving_batch"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = ""
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$receiving_detail_batch(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.ReceivingDetailBatch")
    val primaryKey: String = "rdb_id"
    val cf: String = "receiving_detail_batch"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "rdb_add_time"
    val update_time_field_1: String = "rdb_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$receiving_detail_batch(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.ReceivingDetailBatch")
    val primaryKey: String = "rdb_id"
    val cf: String = "receiving_detail_batch"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "rdb_add_time"
    val update_time_field_1: String = "rdb_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$quality_control(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.QualityControl]
    val primaryKey: String = "qc_id"
    val cf: String = "quality_control"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "qc_add_time"
    val update_time_field_1: String = "qc_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$quality_control(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.QualityControl]
    val primaryKey: String = "qc_id"
    val cf: String = "quality_control"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "qc_add_time"
    val update_time_field_1: String = "qc_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  // fact_receiving_box
  def wms_goodcang_com$gc_receiving_box(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.GcReceivingBox")
    val primaryKey: String = "rb_id"
    val cf: String = "rb"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = ""
    val update_time_field_1: String = "rb_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$receiving_box(database: String, table: String) = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.ReceivingBox]
    val primaryKey: String = "rb_id"
    val cf: String = "rb"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = ""
    val update_time_field_1: String = "rb_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$receiving_box(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.ReceivingBox")
    val primaryKey: String = "rb_id"
    val cf: String = "rb"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = ""
    val update_time_field_1: String = "rb_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  // fact_receiving_log
  def wms_goodcang_com$gc_receiving_log(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.GcReceivingLog")
    val primaryKey: String = "rl_id"
    val cf: String = "gc_receiving_log"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "rl_add_time"
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$receiving_log(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.ReceivingLog")
    val primaryKey: String = "rl_id"
    val cf: String = "receiving_log"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "rl_add_time"
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$receiving_log(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.ReceivingLog")
    val primaryKey: String = "rl_id"
    val cf: String = "receiving_log"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "rl_add_time"
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  // fact_receiving
  def wms_goodcang_com$receiving(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.Receiving")
    val primaryKey: String = "receiving_id"
    val cf: String = "rev"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "receiving_add_time"
    val update_time_field_1: String = "receiving_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$receiving(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.Receiving")
    val primaryKey: String = "receiving_id"
    val cf: String = "rev"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field: String = "receiving_add_time"
    val update_time_field_1: String = "receiving_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$gc_receiving(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.GcReceiving")
    val primaryKey: String = "receiving_id"
    val cf: String = "rev"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "receiving_add_time"
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$order_log(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.OrderLog")
    val primaryKey: String = "ol_id"
    val cf: String = "ol"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = "ol_add_time"
    val update_time_field_1: String = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$order_operation_time(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.OrderOperationTime")
    val primaryKey: String = "oot_id"
    val cf: String = "oot"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field: String = ""
    val update_time_field_1: String = "oot_timestamp"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def oms_goodcang_com$receiving(database: String, table: String) = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_oms.Receiving")
    val primaryKey: String = "receiving_id"
    val cf: String = "rev"
    val datasource_num_id: String = SystemCodeUtil.GC_OMS
    val add_time_field: String = "receiving_add_time"
    val update_time_field_1: String = "receiving_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$ship_order(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.ShipOrder")
    val primaryKey: String = "so_id"
    val cf: String = "so"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "so_add_time"
    val update_time_field_1 = "so_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.OrderAddressBook")
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$order_address_book(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.OrderAddressBook]
    val primaryKey: String = "oab_id"
    val cf: String = "order_address_book"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = ""
    val update_time_field_1 = "oab_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }


  def wms_goodcang_com$bil_income_attach(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.BilIncomeAttach")
    val primaryKey: String = "bia_id"
    val cf: String = "bia"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = ""
    val update_time_field_1 = "bia_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$putaway(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.Putaway")
    val primaryKey: String = "putaway_id"
    val cf: String = "putaway"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "putaway_add_time"
    val update_time_field_1 = "putaway_update_time"
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$gc_putaway_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.GcPutawayDetail")
    val primaryKey: String = "pd_id"
    val cf: String = "pd"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "pd_add_time"
    val update_time_field_1 = "pd_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$putaway_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.PutawayDetail")
    val primaryKey: String = "pd_id"
    val cf: String = "pd"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "pd_add_time"
    val update_time_field_1 = "pd_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$gc_receiving_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.GcReceivingDetail")
    val primaryKey: String = "rd_id"
    val cf: String = "rd"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "rd_add_time"
    val update_time_field_1 = "rd_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$receiving_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.ReceivingDetail")
    val primaryKey: String = "rd_id"
    val cf: String = "rd"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "rd_add_time"
    val update_time_field_1 = "rd_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$gc_receiving_box_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.GcReceivingBoxDetail")
    val primaryKey: String = "rbd_id"
    val cf: String = "rbd"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$receiving_box_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_wms.ReceivingBoxDetail")
    val primaryKey: String = "rbd_id"
    val cf: String = "rbd"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$putaway(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.Putaway")
    val primaryKey: String = "putaway_id"
    val cf: String = "putaway"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "putaway_add_time"
    val update_time_field_1 = "putaway_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$putaway_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.PutawayDetail")
    val primaryKey: String = "pd_id"
    val cf: String = "pd"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "pd_add_time"
    val update_time_field_1 = "pd_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$receiving_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.ReceivingDetail")
    val primaryKey: String = "rd_id"
    val cf: String = "rd"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "rd_add_time"
    val update_time_field_1 = "rd_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$receiving_box_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.ReceivingBoxDetail")
    val primaryKey: String = "rbd_id"
    val cf: String = "rbd"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$order_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.OrderLog")
    val primaryKey: String = "ol_id"
    val cf: String = "ol"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "ol_add_time"
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$order_operation_time(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.OrderOperationTime")
    val primaryKey: String = "oot_id"
    val cf: String = "oot"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = ""
    val update_time_field_1 = "oot_timestamp"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$ship_order(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.zy_wms.ShipOrder")
    val primaryKey: String = "so_id"
    val cf: String = "so"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "so_add_time"
    val update_time_field_1 = "so_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def oms_goodcang_com$receiving_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = Class.forName("com.zongteng.ztetl.entity.gc_oms.ReceivingDetail")
    val primaryKey: String = "rd_id"
    val cf: String = "rd"
    val datasource_num_id: String = SystemCodeUtil.GC_OMS
    val add_time_field = "rd_add_time"
    val update_time_field_1 = "rd_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$flow_volume(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.FlowVolume]
    val primaryKey: String = "fv_id"
    val cf: String = "flow_volume"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "fv_add_time"
    val update_time_field_1 = "fv_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$inventory_batch(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.InventoryBatch]
    val primaryKey: String = "ib_id"
    val cf: String = "inventory_batch"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "ib_add_time"
    val update_time_field_1 = "ib_timestamp"
    val update_time_field_2 = "ib_update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$inventory_batch(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.InventoryBatch]
    val primaryKey: String = "ib_id"
    val cf: String = "inventory_batch"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "ib_add_time"
    val update_time_field_1 = "ib_timestamp"
    val update_time_field_2 = "ib_update_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$inventory_difference(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.InventoryDifference]
    val primaryKey: String = "di_id"
    val cf: String = "inventory_difference"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "create_time"
    val update_time_field_1 = "update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$inventory_difference_detail(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.InventoryDifferenceDetail]
    val primaryKey: String = "idi_id"
    val cf: String = "inventory_difference_detail"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = ""
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$product_inventory(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.ProductInventory]
    val primaryKey: String = "pi_id"
    val cf: String = "product_inventory"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "pi_add_time"
    val update_time_field_1 = "pi_update_time"
    val update_time_field_2 = "change_time"
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$product_inventory(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.ProductInventory]
    val primaryKey: String = "pi_id"
    val cf: String = "product_inventory"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "pi_add_time"
    val update_time_field_1 = "pi_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$take_stock(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.TakeStock]
    val primaryKey: String = "ts_id"
    val cf: String = "take_stock"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "ts_add_time"
    val update_time_field_1 = "ts_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$take_stock(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.TakeStock]
    val primaryKey: String = "ts_id"
    val cf: String = "take_stock"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "ts_add_time"
    val update_time_field_1 = "ts_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$take_stock_assignment(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.TakeStockAssignment]
    val primaryKey: String = "tsa_id"
    val cf: String = "take_stock_assignment"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "tsa_add_time"
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$take_stock_assignment(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.TakeStockAssignment]
    val primaryKey: String = "tsa_id"
    val cf: String = "take_stock_assignment"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "tsa_add_time"
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$take_stock_item(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.TakeStockItem]
    val primaryKey: String = "tsi_id"
    val cf: String = "take_stock_item"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = ""
    val update_time_field_1 = "tsi_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$take_stock_item(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.TakeStockItem]
    val primaryKey: String = "tsi_id"
    val cf: String = "take_stock_item"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = ""
    val update_time_field_1 = "tsi_update_time"
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms_goodcang_com$inventory_batch_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms.InventoryBatchLog]
    val primaryKey: String = "ibl_id"
    val cf: String = "inventory_batch_log"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS
    val add_time_field = "ibl_add_time"
    val update_time_field_1 = ""
    val update_time_field_2 = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }

  def wms$inventory_batch_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.zy_wms.InventoryBatchLog]
    val primaryKey: String = "ibl_id"
    val cf: String = "inventory_batch_log"
    val datasource_num_id: String = SystemCodeUtil.ZY_WMS
    val add_time_field = "ibl_add_time"
    val update_time_field_1 = ""
    val update_time_field_2 = ""
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
