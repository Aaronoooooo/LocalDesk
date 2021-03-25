package com.atguigu.scala.day08

import scala.collection.mutable.ListBuffer

/**
 * @author zxjgreat
 * @create 2020-05-27 10:57
 */
object TestCollection_Seq {

  def main(args: Array[String]): Unit = {

    //不可变Seq-List
//    val list: List[Int] = List(1,2,3,4,5)
//    val list1: List[Int] = list :+ 5
//    println(list1)
    //空集合Nil
//    val list1: List[Int] = 1::2::3::Nil
//    val list2: List[Int] = 4::5::list:::Nil
//    val list3: List[Int] = List.concat(list,list1)
//    println(list1)
//    println(list2)
//    println(list3.mkString(","))
    //可变Seq-ListBuffer

    val buffer: ListBuffer[Int] = ListBuffer(1,2,3,4)
    //增
//    buffer.append(5)
//    println(buffer)
    //删
//    buffer.remove(2)
    //改
//    buffer.update(1,0)

    buffer.foreach(println)
//    println(buffer)













  }

}
