package com.atguigu.scala.chaptor03

/**
 * * @create 2020-05-19 20:19
 */
object Scala01_Operator1 {
  def main(args: Array[String]): Unit = {

    val a = new String("abc")
    val b = new String("abc")

    // scala中String的 ==其实就是equals
    //println(a == b) // true
    //println(a.equals(b)) // true
    // scala中eq用于比较字符串的内存
    println( a eq b ) // false
  }

}
