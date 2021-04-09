package com.atguigu.scala.day03

import scala.util.control.Breaks

/**
 * * @create 2020-05-20 18:36
 */
object TestLoop {

  def main(args: Array[String]): Unit = {

    //for循环，循环守卫
    //    for (i <- Range(1,5) if i != 3){
    //      println(i)
    //    }

    //循环返回值
//    val result = for (i <- Range(1, 5)) yield {
    //      i * 2
    //    }
    //    println(result)
    //

    //循环中断

    Breaks.breakable(
      for (i <- 1 to 5 ){
        if (i == 3){
          Breaks.break()
        }
        println(i)
      }

    )
  }

}
