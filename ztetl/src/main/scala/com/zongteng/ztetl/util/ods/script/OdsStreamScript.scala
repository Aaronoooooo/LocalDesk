package com.zongteng.ztetl.util.ods.script

import java.io.{File, FileOutputStream}

import scala.collection.mutable.ListBuffer

/**
  * 样例类脚本
  */
object OdsStreamScript {

  var createNum = 0
  var existNum = 0
  var deleteNum = 0


  /**
    * 将样例类脚本输出到指定的目录
    * @param datasourceNumId 通过数据库唯一编号（GC_OMS，GC_WMS，ZY_WMS）和mysql表名，获取样例类应该存放的路径
    * @param mysqlTable mysql数据库的表名
    * @param mysqlFiles mysql数据表的字段
    *
    */
  def outputCaseClassScript(datasourceNumId: String, mysqlTable: String, mysqlFiles: ListBuffer[String]) = {


    // 将表名去掉下划线，用驼峰命名法命名
    val caseClassName: String = mysqlTable.split("_").map(str => {
      str.substring(0, 1).toUpperCase + str.substring(1)
    }).mkString("")

    var datasourceNumId2 = datasourceNumId

    if (datasourceNumId.startsWith("gc_lms")) {
      datasourceNumId2 = "gc_lms." +  datasourceNumId
    } else if (datasourceNumId.startsWith("gc_owms")) {
      datasourceNumId2 = "gc_owms." +  datasourceNumId
    } else if (datasourceNumId.startsWith("zy_owms")) {
      datasourceNumId2 = "zy_owms." +  datasourceNumId
    }

    val packageScript = s"package com.zongteng.ztetl.entity.$datasourceNumId2\n"

    val caseClassScriptHeap = s"case class $caseClassName ("

    val caseClassScriptTail = ")"

    val mysqlFileds2: ListBuffer[String] = mysqlFiles.map(_.replace(",", "").trim()).map(filed => {
      var filed2 = filed
      // 因为type是关键字，所以要转为`type`
      if ("type".equalsIgnoreCase(filed2)) {
        filed2 = "`" + filed2 + "`"
      }
      "    " + filed2 + ": String,"
    })

    mysqlFileds2.update(mysqlFileds2.length - 1,  mysqlFileds2.last.substring(0, mysqlFileds2.last.lastIndexOf(",")))

    val caseClassScripts = packageScript :: caseClassScriptHeap :: mysqlFileds2.toList ::: caseClassScriptTail :: Nil

    val caseClassPath: String = getCaseClassPath(datasourceNumId, caseClassName)
    println("caseClass样例类路径：" + caseClassPath)

    // 如果是删除所有文件，那么不需要输出了
    var stream: FileOutputStream = null
    try {
      stream = new FileOutputStream(caseClassPath)
      caseClassScripts.foreach((x: String) => {
        stream.write(x.getBytes)
        stream.write("\r\n".getBytes()) // 换行
      })
    } catch {
      case e: Exception => println("sqoop_io异常：检查目录是否存在")
    } finally {
      stream.close()
    }

  }

  /**
    * 通过数据库唯一编号（GC_OMS，GC_WMS，ZY_WMS）和mysql表名，获取样例类应该存放的路径
    * @param datasourceNumId GC_OMS，GC_WMS，ZY_WMS
    * @param caseClassName  样例类
    */
  def getCaseClassPath(datasourceNumId: String, caseClassName: String) = {

    var datasourceNumId2 = datasourceNumId

    if (datasourceNumId.startsWith("gc_lms")) {
      datasourceNumId2 = "gc_lms\\" +  datasourceNumId
    } else if (datasourceNumId.startsWith("gc_owms")) {
      datasourceNumId2 = "gc_owms\\" +  datasourceNumId
    } else if (datasourceNumId.startsWith("zy_owms")) {
      datasourceNumId2 = "zy_owms\\" +  datasourceNumId
    }

    val classFile: String = "com\\zongteng\\ztetl\\entity\\" + datasourceNumId2
    val caseClassPath: String = getClass.getClassLoader.getResource(classFile).getPath + "/" + caseClassName + ".scala"

    //  类路径下的：    F:/project/IdeaProjects2/trunk/target/classes/com/zongteng/ztetl/entity/gc_wms/AdvancePickingDetail.scala
    //  idea路径下的：  F:\project\IdeaProjects2\trunk\src\main\scala\com\zongteng\ztetl\entity\gc_wms
    val path: String = caseClassPath.replace("%5c", "/").replace("target/classes", "src\\main\\scala")

    // 如果文件已经存在，那么删除
    val file = new File(path)


    if (file.isFile && !file.exists()) {
      createNum += 1
    }

    if (file.isFile && file.exists()) {
      existNum += 1
    }


    path
  }


  private def deleteFile(datasourceNumId: String, caseClassName: String) = {

    var datasourceNumId2 = datasourceNumId

    if (datasourceNumId.startsWith("gc_lms")) {
      datasourceNumId2 = "gc_lms\\" +  datasourceNumId
    } else if (datasourceNumId.startsWith("gc_owms")) {
      datasourceNumId2 = "gc_owms\\" +  datasourceNumId
    } else if (datasourceNumId.startsWith("zy_owms")) {
      datasourceNumId2 = "zy_owms\\" +  datasourceNumId
    }

    val classFile: String = "com\\zongteng\\ztetl\\entity\\" + datasourceNumId2
    val caseClassPath: String = getClass.getClassLoader.getResource(classFile).getPath + "/" + caseClassName + ".scala"

    //  类路径下的：    F:/project/IdeaProjects2/trunk/target/classes/com/zongteng/ztetl/entity/gc_wms/AdvancePickingDetail.scala
    //  idea路径下的：  F:\project\IdeaProjects2\trunk\src\main\scala\com\zongteng\ztetl\entity\gc_wms
    val path: String = caseClassPath.replace("%5c", "/").replace("target/classes", "src\\main\\scala")

    // 如果文件已经存在，那么删除
    val file = new File(path)
    if (file.isFile && file.exists()) {
      file.deleteOnExit()
      deleteNum += 1
    }

    path
  }

  def main(args: Array[String]): Unit = {


  }


}
