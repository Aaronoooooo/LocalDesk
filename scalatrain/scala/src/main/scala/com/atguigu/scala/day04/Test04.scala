package com.atguigu.scala.day04

/**
 * 函数作为返回值
 * * @create 2020-05-22 18:52
 */
object Test04 {

  def main(args: Array[String]): Unit = {

    //    def test() ={
    //      def fun(i : Int): Int ={
    //        i * 2
    //      }
    //      fun _
    //    }
    //
    //    println(test()(20))

    def test() = {
      def fun(i: Int) = {
        i * 2
      }
      fun _
    }

    println(test()(10))

  }

}
