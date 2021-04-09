package com.atguigu.scala.day09

import scala.collection.mutable

/**
 * * @create 2020-05-29 20:32
 */
object TestCollection_Method1 {

  def main(args: Array[String]): Unit = {
//    val list = List(1,2,3,4)
//
////     fold方法直接获取最终的结果
////
////     scan类似于 fold方法，但是会将中间的处理结果也保留下来
////     集合扫描
////     0, 1, 3, 6, 10
//    println("scan => " + list.scan(0)(_+_))
////     集合扫描(左)
//    println("scanLeft => " + list.scanLeft(0)(_+_))
////     集合扫描(右)
//    println("scanRight => " + list.scanRight(0)(_+_))



    // BlockingQueue(阻塞式队列),
    // Deque（双端队列）
    // Kafka 如何保证数据的有序？

//            val que = new mutable.Queue[String]()
//            // 添加元素
//            que.enqueue("a", "b", "c")
//    //        val que1: mutable.Queue[String] = que += "d"
//    //        println(que eq que1)
//            // 获取元素
//            println(que.dequeue())
//            println(que.dequeue())
//            println(que.dequeue())






//    val result1 = (0 to 100).map{x => Thread.currentThread.getName}

    val result2 = (0 to 100).par.map{x => Thread.currentThread.getName}
////
//    println(result1)
    println(result2)
  }

}
