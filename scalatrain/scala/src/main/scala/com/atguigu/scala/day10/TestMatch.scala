package com.atguigu.scala.day10

/**
 * * @create 2020-05-30 9:57
 */
object TestMatch {
  def main(args: Array[String]): Unit = {

    //case _ 的分支一般写在所有分支的最后，模仿default语法
    //如果所有的分支都不匹配，还没有case _分支，那么会发生错误
//    var a: Int = 10
//    var b: Int = 20
//    var operator: Char = 'd'
//    var result = operator match {
//
//      case '+' => a + b
//      case '-' => a - b
//      case '*' => a * b
//      case '/' => a / b
//      case _ => "illegal"
//    }
//    println(result)

    val list = List(("a",1),("b",2),("c",3))

    val result: List[(String, Int)] = list.map(
      (t) => {
        (t._1, t._2 * 2)
      }
    )
    println(result)

    val newList: List[(String, Int)] = list.map {
      case (word, count) => {
        (word, count * 2)
      }
    }
    println(newList)


  }

}
