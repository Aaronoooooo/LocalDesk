package com.zongteng.ztetl.common

import java.io.FileInputStream
import java.util

import com.zongteng.ztetl.api.HBaseConfig
import com.zongteng.ztetl.entity.common.ParDatasourceList
import com.zongteng.ztetl.util.{DataNullUtil, DateUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.poi.xssf.usermodel.{XSSFRow, XSSFWorkbook}

import scala.collection.mutable.ListBuffer

object ParDatasourceListToHbase {
  //文件路径
  val excel_path = "F:\\workDoucument\\par_val_list\\par_datasoure_list.xlsx"

  //字段值
  var datasource: String = _
  var datasource_num_id: String = _
  var datasource_short_name: String = _
  var database_name: String = _

  var row_wid: String = _

  // 最终得到的put集合
  val putList: util.ArrayList[Put] = new java.util.ArrayList[Put]
  // hbase列族
  val column_family: String = "datasoure_list"
  // dw层hbase表名
  val table_name: String = "par_datasource_list"

  def main(args: Array[String]): Unit = {
    //统计条数
    var excel_num = 0
    //获取文档中数据
    getInfoByExel(excel_path).foreach((parDatasourceList: ParDatasourceList) => {
      //解析样例类
      evalVal(parDatasourceList)
      //生成hbase的put语句
      val put: Put = getHbasePut()
      putList.add(put)
      excel_num += 1
    })

    //将数据写入hbase中
    insertToHbase(table_name,putList)

    println("一共插入了" + excel_num + "条数据")
  }

  /**
    * 将数据写入hbase中
    * @return
    */
  def insertToHbase(name:String,putList: util.ArrayList[Put]): Int = {
    var num=0
    try {
      //获取hbase连接
      val connection: Connection = HBaseConfig.getConnection()
      val tableName: TableName = TableName.valueOf(name)
      val table: Table = connection.getTable(tableName)

      for (i <- 0 until putList.size) {
        val put: Put = putList.get(i)
        table.put(put)
        println(name+"表数据新增成功" + put)
        num+=1
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
    * @param parDatasourceList
    */
  def evalVal(parDatasourceList: ParDatasourceList) = {
    datasource = parDatasourceList.datasource
    datasource_num_id=parDatasourceList.datasource_num_id
    datasource_short_name=parDatasourceList.datasource_short_name
    database_name=parDatasourceList.database_name

    row_wid = parDatasourceList.datasource_num_id

    println(row_wid + " " + datasource + " " + datasource_num_id + " " + datasource_short_name + " " + database_name)
  }

  /**
    * 生成hbase的put语句
    */
  def getHbasePut() = {

    val put: Put = new Put(Bytes.toBytes(row_wid))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("datasource"), Bytes.toBytes(datasource))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("datasource_short_name"), Bytes.toBytes(datasource_short_name))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("database_name"), Bytes.toBytes(database_name))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_insert_dt"), Bytes.toBytes(DateUtil.getNow()))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_update_dt"), Bytes.toBytes(DateUtil.getNow()))

    put
  }


  /**
    * 判断数据准确性
    * @param row
    * @param datasource
    * @param datasourceNumId
    * @param datasourceShortName
    * @param databaseName
    */
  def checkInfoExcel(row: Int, datasource: String, datasourceNumId: String, datasourceShortName: String, databaseName: String) = {
    val errorMessage = "，请检查并填上对应的信息。如果是空行，请删除"
    val rowMessage = s"第${row}行"

    assert(datasource != "", s"${rowMessage}的源系统名称为空${errorMessage}")
    assert(datasourceNumId != "", s"${rowMessage}的源系统编号为空${errorMessage}")
    assert(datasourceShortName != "", s"${rowMessage}的源系统缩写为空${errorMessage}")
    assert(databaseName != "", s"${rowMessage}的mysql数据库名称信息为空${errorMessage}")
  }


  //获取excel表中的数据
  def getInfoByExel(excel_path: String) = {

    val infos: ListBuffer[ParDatasourceList] = ListBuffer[ParDatasourceList]()

    // 维护文档所在的路径
    val filePath = excel_path
    val fs = new FileInputStream(filePath)
    val wb: XSSFWorkbook = new XSSFWorkbook(fs)
    val sheet = wb.getSheetAt(0)
    val maxRow = sheet.getLastRowNum()

    for (i <- 1 to maxRow) {
      val row: XSSFRow = sheet.getRow(i)

      val datasource: String = DataNullUtil.nvlObject(row.getCell(0))
      val datasourceNumId: String = DataNullUtil.nvlObject(row.getCell(1)).replaceAll("\\.0", "")
      val datasourceShortName: String = DataNullUtil.nvlObject(row.getCell(2))
      val databaseName: String = DataNullUtil.nvlObject(row.getCell(3))


      checkInfoExcel(i + 1, datasource, datasourceNumId, datasourceShortName, databaseName)

      infos += ParDatasourceList(datasource, datasourceNumId, datasourceShortName, databaseName)
    }

    fs.close()
    infos
  }
}
