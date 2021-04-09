package com.atguigu.scala.day09

import scala.collection.mutable

/**
 * * @create 2020-05-29 15:10
 */
object TestFoldLeft {
  def main(args: Array[String]): Unit = {

    // TODO Scala - 集合 - 常用方法 - 折叠

    // 将集合之外的数据和集合内部的数据进行聚合的操作，称之为折叠
    // 聚合数据的方式依然为两两操作
    val dataList = List(1, 2, 3, 4, 5)
    // fold方法存在函数柯里化，有2个参数列表
    // 第一个参数列表中的参数 => z : A1【z为zero，表示数据处理的初始值】
    // 第二个参数列表中的参数 => (A1,A1)=>A1【聚合数据的逻辑】

    // fold方法在进行数据处理时，外部的数据应该和集合内部的数据的类型保持一致
    //

    // 从源码的角度讲，fold方法的底层其实就是foldLeft
    // fold,foldLeft,foldRight方法的返回值类型为初始值的类型
//    val i: Int = dataList.fold(8)(_ - _)
//    //    (((((8-1)-2)-3)-4)-5)=-7
//    val i1: Int = dataList.foldLeft(8)(_ - _)
//    //    (((((8-1)-2)-3)-4)-5)=-7
//    val i2: Int = dataList.foldRight(8)(_ - _)
//    //(1-(2-(3-(4-(5-8)))))=-5
//    println(i)
//    println(i1)
//    println(i2)

        val map1: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3)
        val map2: mutable.Map[String, Int] = mutable.Map("a" -> 4, "d" -> 5, "c" -> 6)

        val result: mutable.Map[String, Int] = map1.foldLeft(map2)(
          (map,kv)=>{

//            val k = kv._1
//            val v = kv._2
//            val newVal = map.getOrElse(k,0) + v
//            map(k) = newVal
            map.update(kv._1,map.getOrElse(kv._1,0) + kv._2)
            map
        })
        println(result)

  }

}
