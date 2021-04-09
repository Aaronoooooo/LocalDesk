package com.atguigu.scala.day10

/**
 * * @create 2020-05-31 8:52
 */
object TestMatch1 {

  def main(args: Array[String]): Unit = {
    //模式匹配：匹配规则
    val list: List[Int] = List(1, 2, 5, 6, 7)
    val list1: List[Int] = List(1, 2)
    val list2: List[Int] = List(1)

    // 1::2::3::Nil
    list2 match {
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }

    for (list <- Array(
      List(0), // 0
      List(1, 0), // 1，0
      List(0, 0, 0), // 0 ...
      List(1, 0, 0), // something else
      List(88))) { // something else
      val result = list match {
        case List(0) => "0" //匹配List(0)
        case List(x, y) => x + "," + y //匹配有两个元素的List
        case List(0, _*) => "0 ..."
        case _ => "something else"
      }

      //println(result)
    }

    // TODO 匹配数组
    for (arr <- Array(
      Array(0), // 0
      Array(1, 0), // 1，0
      Array(0, 1, 0), // 以0开头的数组
      Array(1, 1, 0), // something else
      Array(1, 1, 0, 1),// something else
      Array("hello", 90))) { // hello，90

      val result = arr match {
        case Array(0) => "0" //匹配Array(0) 这个数组
        case Array(x, y) => x + "," + y //匹配有两个元素的数组，然后将将元素值赋给对应的x,y
        case Array(0, _*) => "以0开头的数组" //匹配以0开头和数组
        case _ => "something else"
      }
      //println("result = " + result)
    }

    // TODO 匹配类型
    // 下划线的作用省略参数，因为逻辑中不使用参数，所以可以省略
    // 但是需要这个参数，那么可以起个名字
    // 类型匹配不考虑泛型的:数组的泛型其实是类型的一部分。
    // 底层实现采用类型判断
    def describe1(x: Any) = {
      val result = x match {
        case i: Int => "Int"
        case s: String => "String hello"
        case m: List[String] => "List"
        case c: Array[Int] => "Array[Int]"
        case someThing => "something else " + someThing
      }
      println( result )
    }

    // TODO 匹配常量 : final, val
    def describe(x: Any) = {
      val result = x match {
        case 5 => "Int five"
        case "hello" => "String hello"
        case true => "Boolean true"
        case '+' => "Char +"
      }
      println( result )
    }
    //describe("abc")
    // TODO 匹配元组
    for (tuple <- Array(
      (0, 1), // 0 ...
      (1, 0), // 10
      (1, 1), // 1 1
      (1, 0, 2))) { // something else
      val result = tuple match {
        case (0, _) => "0 ..." //是第一个元素是0的元组
        case (y, 0) => "" + y + "0" // 匹配后一个元素是0的对偶元组
        case (a, b) => "" + a + " " + b
        case _ => "something else" //默认
      }
      println(result)
    }
  }
}
