package com.atguigu.scala.chaptor01

/**
 * @author zxjgreat
 * @create 2020-05-18 21:31
 */
object InputTest {

  def main(args: Array[String]): Unit = {
//    val age : Int = StdIn.readInt()
//    println("学生的年龄为：" + age)
    scala.io.Source.fromFile("input/user.json").foreach(
      line => {
        print(line)
      }
    )
    scala.io.Source.fromFile("input/user.json").getLines()
  }




}
