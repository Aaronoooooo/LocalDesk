package com.atguigu.scala.day08

/**
 * @author zxjgreat
 * @create 2020-05-27 20:03
 */
object TestCollection_Method {
  def main(args: Array[String]): Unit = {
//    val list = List(1,2,3,4)

    // 获取集合的长度
    //println(list.length)
    //println(list.size)

    // 遍历数据
    //list.foreach(println)
    //println(list.mkString(","))
    // val iterator: Iterator[Int] = list.iterator

    // 判断是否为空
    //println(list.isEmpty)

    // 简单运算
//    println(list.sum)
//    println(list.max)
//    println(list.min)
//    // 乘积
//    println(list.product)
    // TODO Scala - 集合 - 常用方法

    val list = List(1,2,3,4)


    // 函数式编程中使用最多的算法其实就是递归算法

    // 头 => 1
    //println(list.head)
    // 尾 => List(2,3,4)
    // 尾 -> 尾 -> 尾
    //println(list.tail)

    // 最后一个 => 4
    //println(list.last)
    // 初始 => List(1,2,3)
    //println(list.init)

    // 反转
    //println(list.reverse)
    // println(list.reverse.head)

    // 判断数据是否存在
    // println(list.contains(5))

    // 数据去重
    //println(list.toSet)
    //println(list.distinct)

    // 取(拿)数据
    //println(list.take(3))
    // 从右边拿
    // 1,2,3,4
    // 4, 3, 2
    // 2, 3, 4
    //println(list.takeRight(3))

    // 丢弃数据
    // 1,2,3,4
    println(list.drop(2))
    println(list.dropRight(2))


  }

}
