package com.atguigu.scala.chaptor01

/**
 * * @create 2020-05-18 10:48
 */
object ScalaTest_DataType {

  def main(args: Array[String]): Unit = {

    //数据类型  -AnyVal
    val b: Byte = 10
    val s: Short = 10
    val i: Int = 10
    val l: Long = 10L
    val f: Float = 10f
    val d: Double = 1.0
    val c: Char = 'a'
    val boolean: Boolean = true
    val u: Unit = () //Unit的值为()

    val i1 = 20
    println(i1) //整数型默认Int

    val d1 = 2.0
    println(d1) //浮点型默认Double

    //引用对象类型 -AnyRef
  }
}
