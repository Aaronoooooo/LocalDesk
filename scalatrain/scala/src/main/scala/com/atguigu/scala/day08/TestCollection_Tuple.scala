package com.atguigu.scala.day08

import scala.collection.mutable

/**
 * * @create 2020-05-27 19:45
 */
object TestCollection_Tuple {

  def main(args: Array[String]): Unit = {
    // 如果将无关的数据当成一个整体来使用，
    // 封装bean,json,集合都不是一个好的选择。
    // Scala中会采用一种特殊的结构来进行封装。
    // 这个特殊的结构就是采用小括号, 称之为元组（元素的组合）:Tuple
    val tuple: (Int, String, Int, Int, String, String) = (1,"zhangsan",2,3,"lisi","wangwu")

//    println(tuple._1)
//    println(tuple._2)
//    println(tuple._3)
//    println(tuple._4)
//    println(tuple._5)
//    println(tuple._6)

//    val iterator: Iterator[Any] = tuple.productIterator
//    while (iterator.hasNext){
//      println(iterator.next())
//    }

    //索引
//    println(tuple.productElement(4))

    val map: mutable.Map[String, Int] = mutable.Map( ("a", 1), ("b", 2), ("c", 3))

    for (kv <- map){
      println(kv._1 + "=" + kv._2)
    }







  }

}
