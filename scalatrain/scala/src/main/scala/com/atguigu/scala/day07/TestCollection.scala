package com.atguigu.scala.day07

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 集合-数组
 *
 * * @create 2020-05-26 22:04
 */
object TestCollection {

  def main(args: Array[String]): Unit = {

    val arr01 = new Array[Int](4)
    println(arr01.length) // 4

    //（2）数组赋值
    //（2.1）修改某个元素的值
    arr01(3) = 10
    val i = 10
    arr01(i/3) = 20
    println(arr01.mkString(","))



    //可变集合Array
//    val array = new Array[String](5)
//    array(0) = "zhangsan"
//    array(1) = "zhangsan1"
//    array(2) = "zhangsan2"
//    array(3) = "zhangsan3"
//    array(4) = "zhangsan4"

    //遍历数组中的数据
//    for (s <- array){
//      println(s)
//    }

    //按照指定格式生成字符串
//    println(array.mkString(","))


    //集合Array
//    val array = Array(1,2,3,4)
//    // 添加数据
//    // 使用 :+ 5 表示向数组的后面添加数据
//    val newArray = array :+ 5
//    println(newArray.mkString(","))
//
//    // 使用 +: 表示向数组的前面添加数据
//    val newArray1 = 5 +: array
//    println(newArray1.mkString(","))

    //可变集合ArrayBuffer
//    val array = new ArrayBuffer[String]()
//    array.append("a")
//    array.append("b")
//    println(array.mkString(","))

    // Scala
    // 可变数组
    //    内存存储的数据可以动态操作，而不会产生新的集合
    //    可变数组提供了大量对数据操作的方法，基本上方法名都是单词
    // 不可变数组
    //    对数据的操作都会产生新的集合。
    //    提供对数据的操作方法相对来说，比较少，而且都是一些符号。

    // StringBuilder => String
    // 可变数组和不可变数组的互相转换。
    // 不可变 => 可变
//    val array = Array(1,2,3)
//    val buffer: mutable.Buffer[Int] = array.toBuffer
//    // 可变 => 不可变
//    val array1 = ArrayBuffer(1,2,3)
//    val array2: Array[Int] = array1.toArray

  }

}
