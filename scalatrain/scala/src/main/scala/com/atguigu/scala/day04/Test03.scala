package com.atguigu.scala.day04

/**
 * 尾递归
 *
 * * @create 2020-05-22 16:43
 */
object Test03 {

  def main(args: Array[String]): Unit = {

    def fun(num: Int, result: Int): Int = {
      if (num < 1) {

        result
      } else {
        fun(num - 1, num + result)
      }

    }

    println(fun(10, 0))
  }

}
