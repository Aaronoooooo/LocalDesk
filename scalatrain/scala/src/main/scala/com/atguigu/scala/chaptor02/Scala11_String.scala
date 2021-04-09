package com.atguigu.scala.chaptor02

/**
 * 字符串的操作
 *
 * * @create 2020-05-19 19:07
 */
object Scala11_String {
  def main(args: Array[String]): Unit = {

    val name: String = "zhangxiujun"
    val age: Int = 26

    //字符串的拼接
    println("name: " + name + ", age: " + age)

    //传值字符串
    printf("name = %s , age = %s", name, age)

    //插值字符串
    println(s"name = $name , age = $age")

    //多行字符串
    println(
      s"""
         |("name = $name , age = $age")
         |""".stripMargin
    )
  }

}
