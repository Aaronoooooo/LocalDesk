package com.atguigu.scala.chaptor01

import java.io.{File, PrintWriter}

/**
 * * @create 2020-05-18 21:44
 */
object OutputTest {
  def main(args: Array[String]): Unit = {

    val writer = new PrintWriter(new File("output/test.txt)"))
    writer.write("zhangxiujun")
    writer.close()

   }
}
