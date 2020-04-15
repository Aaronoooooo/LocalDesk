package com.zongteng.ztetl.common

import java.io.FileInputStream
import java.util

import com.zongteng.ztetl.api.HBaseConfig
import com.zongteng.ztetl.entity.common.ParValList
import com.zongteng.ztetl.util.{DataNullUtil, DateUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.poi.xssf.usermodel.{XSSFRow, XSSFSheet, XSSFWorkbook}

import scala.collection.mutable.ListBuffer

/**
  * 读取excel中的值列表数据,写入hbase中
  */
object ParValListToHbase {
  //文件路径
  val excel_path = "F:\\workDoucument\\svn\\Delivery\\E 开发实现\\开发清单\\par_val_list值列表.xlsx"
  //val excel_path = ""

  // 读取excel文档中sheet个数，动态的设置
  val sheetNum: Int = 1

  //字段值
  var datasource_num_id: String = _
  var vl_type: String = _
  var vl_value: String = _
  var vl_name: String = _
  var vl_bi_name: String = _
  var vl_datasource_table: String = _
  var vl_memo: String = _

  var row_wid: String = _

  // 最终得到的put集合
  val putList: util.ArrayList[Put] = new java.util.ArrayList[Put]
  // hbase列族
  val column_family: String = "list"
  // dw层hbase表名
  val table_name: String = "par_val_list"

  def main(args: Array[String]): Unit = {

    //统计条数
    var excel_num = 0
    //获取文档中数据
    getInfoByExel(excel_path).foreach((parValList: ParValList) => {
      //解析样例类
      evalVal(parValList)
      //生成hbase的put语句
      val put: Put = getHbasePut()
      putList.add(put)
      excel_num += 1
    })

    //将数据写入hbase中
    insertToHbase(table_name, putList)

    println("一共插入了" + excel_num + "条数据")
  }

  /**
    * 将数据写入hbase中
    *
    * @return
    */
  def insertToHbase(name: String, putList: util.ArrayList[Put]): Int = {
    var num = 0
    try {
      //获取hbase连接
      val connection: Connection = HBaseConfig.getConnection()
      val tableName: TableName = TableName.valueOf(name)
      val table: Table = connection.getTable(tableName)

      for (i <- 0 until putList.size) {
        val put: Put = putList.get(i)
        table.put(put)
        println(name + "表数据新增成功" + put)
        num += 1
      }
      //关闭表
      table.close()
      connection.close()
    } catch {
      case e: Exception => e.getMessage
    }
    num
  }

  /**
    * 解析样例类
    *
    * @param parValList
    */
  def evalVal(parValList: ParValList) = {
    datasource_num_id = parValList.datasource_num_id
    vl_type = parValList.vl_type
    vl_value = parValList.vl_value
    vl_name = parValList.vl_name
    vl_bi_name = parValList.vl_bi_name
    vl_datasource_table = parValList.vl_datasource_table
    vl_memo = parValList.vl_memo

    row_wid = parValList.datasource_num_id.concat("_").concat(parValList.vl_type).concat("_").concat(parValList.vl_value).concat("_").concat(parValList.vl_datasource_table)

    println(row_wid + " " + datasource_num_id + " " + vl_type + " " + vl_value + " " + vl_name + " " + vl_bi_name + " " + vl_datasource_table + " " + vl_memo)
  }

  /**
    * 生成hbase的put语句
    */
  def getHbasePut() = {

    val put: Put = new Put(Bytes.toBytes(row_wid))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("datasource_num_id"), Bytes.toBytes(datasource_num_id))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("vl_type"), Bytes.toBytes(vl_type))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("vl_value"), Bytes.toBytes(vl_value))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("vl_name"), Bytes.toBytes(vl_name))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("vl_bi_name"), Bytes.toBytes(vl_bi_name))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("vl_datasource_table"), Bytes.toBytes(vl_datasource_table))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("vl_memo"), Bytes.toBytes(vl_memo))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_insert_dt"), Bytes.toBytes(DateUtil.getNow()))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_update_dt"), Bytes.toBytes(DateUtil.getNow()))

    put
  }


  /**
    * 判断数据准确性
    *
    * @param row
    * @param datasourceNumId
    * @param vlType
    * @param VlValue
    * @param VlName
    * @param VlBiName
    * @param VlDatasourceTable
    */
  def checkInfoExcel(row: Int, datasourceNumId: String, vlType: String, VlValue: String, VlName: String, VlBiName: String, VlDatasourceTable: String) = {
    val errorMessage = "，请检查并填上对应的信息。如果是空行，请删除"
    val rowMessage = s"第${row}行"

    assert(datasourceNumId != "", s"${rowMessage}的源系统编号为空${errorMessage}")
    assert(vlType != "", s"${rowMessage}的字段信息为空${errorMessage}")
    assert(VlValue != "", s"${rowMessage}的字段值信息为空${errorMessage}")
    assert(VlName != "", s"${rowMessage}的字段显示值信息为空${errorMessage}")
    assert(VlBiName != "", s"${rowMessage}的字段报表显示值信息为空${errorMessage}")
    assert(VlDatasourceTable != "", s"${rowMessage}的ods层表名信息为空${errorMessage}")
  }


  //获取excel表中的数据
  def getInfoByExel(excel_path: String) = {

    // 维护文档所在的路径
    val filePath = excel_path
    val fs = new FileInputStream(filePath)
    val wb: XSSFWorkbook = new XSSFWorkbook(fs)

    val infos: ListBuffer[ParValList] = ListBuffer[ParValList]()

    //获取一个文件中的所有文档
    val sheets: Int = wb.getNumberOfSheets

    for (num <- 0 until sheets; if num <= sheetNum - 1) {
      println(s"第${num+1}个文档")
      val sheet: XSSFSheet = wb.getSheetAt(num)
      val maxRow = sheet.getLastRowNum()

      for (i <- 1 to maxRow) {
        val row: XSSFRow = sheet.getRow(i)

        val datasourceNumId: String = DataNullUtil.nvlObject(row.getCell(0)).replaceAll("\\.0", "")
        val vlType: String = DataNullUtil.nvlObject(row.getCell(1))
        val VlValue: String = DataNullUtil.nvlObject(row.getCell(2)).replaceAll("\\.0", "")
        val VlName: String = DataNullUtil.nvlObject(row.getCell(3))
        val VlBiName: String = DataNullUtil.nvlObject(row.getCell(4))
        val VlDatasourceTable: String = DataNullUtil.nvlObject(row.getCell(5))
        val VlMemo: String = DataNullUtil.nvlObject(row.getCell(6))


        checkInfoExcel(i + 1, datasourceNumId, vlType, VlValue, VlName, VlBiName, VlDatasourceTable)

        infos += ParValList(datasourceNumId, vlType, VlValue, VlName, VlBiName, VlDatasourceTable, VlMemo)
      }
    }
    fs.close()
    infos
  }



}
