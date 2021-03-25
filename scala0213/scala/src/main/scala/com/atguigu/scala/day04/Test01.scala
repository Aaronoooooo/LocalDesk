package com.atguigu.scala.day04

/**
 * @author zxjgreat
 * @create 2020-05-22 9:46
 */
object Test01 {

  def main(args: Array[String]): Unit = {
    //    //闭包
    //    def test()={
    //      val a = 20
    //      def fun()={
    //
    //        a * 5
    //      }
    //      fun _
    //    }
    //
    //    println(test()())

    //闭包
    //    def test()={
    //      val i = 10
    //      def fun(): Int ={
    //        i * 5
    //      }
    //      fun _
    //    }
    //
    //    println(test()())
    //闭包
//    def test() = {
//      val a = "zxj"
//
//      def fun() = {
//        a + " love pyy!!!"
//      }
//      fun _
//    }
//
//    println(test()())
    def fun(i : Int) ={
      def test(j : Int): Int ={
        i * j
      }
      test _
    }

    println(fun(10)(52))
  }

}
