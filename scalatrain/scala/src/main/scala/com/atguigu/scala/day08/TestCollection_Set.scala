package com.atguigu.scala.day08

import scala.collection.mutable

/**
 * set
 *
 * * @create 2020-05-27 19:06
 */
object TestCollection_Set {
  def main(args: Array[String]): Unit = {

    //set无序的无序性，不可重复性
//    val set: Set[Int] = Set(1,2,3,4,5,6,7,8)
//    val set1: Set[Int] = Set(1,23,5,1,23)
//    println(set)
//    println(set1)

    //可变集合set
    val set = mutable.Set(1,2,3,4)
//    set.add(5)
//    set.update(5,true)
//    set.update(2,false)
    //删除的是元素
    set.remove(1)
    println(set)










  }

}
