package com.atguigu.scala.chaptor04

/**
 * @author zxjgreat
 * @create 2020-05-19 21:23
 */
object Scala05_Loop {

  def main(args: Array[String]): Unit = {

    //遍历集合[1，2，3，4，5]
//    for (i <- 1 to 5){
//      println(i)
//    }
    //遍历集合[1，2，3，4，5)
//    for (i <- 1 until 5){
//      println(i)
//    }
    //遍历集合[1，2，3，4，5)
//    for (i <- Range(1,5)){
//      println(i)
//    }

    // for循环默认情况下是一个一个数据循环。
    // Range可以传递三个参数：start, end(until), step
    //每隔一个遍历一个元素
//    for (i <- Range(1,5,2)){
//      println(i)
//    }
    for (i <- 1 to 5 by 2){
      println(i)
    }

  }

}
