package com.zongteng.ztetl.util.ods.script

import java.io.FileOutputStream
import java.nio.ByteBuffer

import scala.io.Source



object AllowMysqlTableScript {

  def main(args: Array[String]): Unit = {
    outPutAllowMysqlTableScript(List("aa", "bb"), "picking")
  }

  def outPutAllowMysqlTableScript(odsTables: List[String], theme: String) = {
    val classPath = getCaseClassPath()
    val beforeUpdateText: String = Source.fromFile(classPath, "utf8").mkString

    val script: String = getScirpt(odsTables, theme)

    val lastIndex = beforeUpdateText.lastIndexOf("}")
    val afterUpateText = beforeUpdateText.substring(0, lastIndex) + script + "\n}"


    val f = new FileOutputStream(classPath).getChannel
    f.write(ByteBuffer.wrap(afterUpateText.getBytes))
    f.close
  }

  private def getScirpt(odsTables: List[String], theme: String) = {
    val odsTables2 = odsTables.map("      " + addDoubleQuotes(_))
    val theme2 = theme.substring(0, 1).toLowerCase + theme.substring(1)
    s"  def ${theme2}Tables = {\n    Array(${odsTables2.mkString(",\n")}\n    )\n  }"
  }

  /**
    * 获取AllowMysqlTable.scala的路径
    * @return
    */
  private def getCaseClassPath() = {
    val classFile: String = "com\\zongteng\\ztetl\\util\\stream"
    val path: String = getClass.getClassLoader.getResource(classFile).getPath
    val classPath: String = path.replace("%5c", "/").
      replace("target/classes", "src\\main\\scala") + s"\\" + s"AllowMysqlTable.scala"

    println(classPath)

    classPath
  }

  /**
    * 字符串左右添加双引号
    * @param str
    * @return
    */
  private def addDoubleQuotes(str: String) = {
    "\"" + str + "\""
  }

}
