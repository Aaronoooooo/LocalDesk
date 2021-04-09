package com.atguigu.scala.chaptor04

/**
 * * @create 2020-05-19 21:16
 */
object Scala02_Flow1 {

  def main(args: Array[String]): Unit = {
    val age = 26

    val result = if (age < 18) {
      "未成年"
    } else if (age < 36) {
      //println("青年")
      "青年"
    } else if (age < 55) {
      "中年"
    } else {
      "老年"
    }

    println(result)
  }

}
