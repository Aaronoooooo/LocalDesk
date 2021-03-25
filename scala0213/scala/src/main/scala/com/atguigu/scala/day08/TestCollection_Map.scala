package com.atguigu.scala.day08

import scala.collection.mutable

/**
 * @author zxjgreat
 * @create 2020-05-27 19:21
 */
object TestCollection_Map {
  def main(args: Array[String]): Unit = {
    //map无序,不可重复
    //    val map: Map[String, Int] = Map("a"->1,"b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5 )
    //    println(map)

    val map: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3)
//    // 添加数据
//    map.put("d", 4)
//    // 修改数据
//    //map.update("d", 5)
//    map("a") = 6
//    // 删除数据
//    map.remove("a")
//    println(map)
    val maybeInt: Option[Int] = map.get("a")
    // Option : 选项类型
    //          有值：Some，根据key可以获取值
    //          无值：None, 根据key获取不到值
//    println(maybeInt)


//    println(map.getOrElse("d", -1))

  }


}
